package dev.sdp.connect

import dev.sdp.core.*
import org.apache.spark.connect.proto as sc
import zio.test.*

/** F8b: golden parse-back tests — every encoded command must survive
  * `parseFrom(toByteArray)` (the canonical protobuf-java reader) and carry
  * the expected field values.
  */
object PipelineProtoEncoderSpec extends ZIOSpecDefault:

  private val graphId = "graph-test-1"

  private val manifest = PipelineManifest.fromGraph(
    PipelineGraph(
      Map(
        "bronze" -> PipelineNode.Table("bronze", "delta"),
        "silver" -> PipelineNode.StreamingTable("silver", "delta"),
        "gold"   -> PipelineNode.MaterializedView("gold", "SELECT id, count(*) FROM silver GROUP BY id"),
        "tmp"    -> PipelineNode.TemporaryView("tmp", "SELECT * FROM bronze"),
        "wide"   -> PipelineNode.StreamingTable("wide", "delta"),
      ),
      Set(
        DependencyEdge("bronze", "silver"),
        DependencyEdge("bronze", "wide"),
        DependencyEdge("silver", "wide"),
      ),
    )
  )

  private def reparse(cmd: sc.PipelineCommand): sc.PipelineCommand =
    sc.PipelineCommand.parseFrom(cmd.toByteArray)

  private val commands = PipelineProtoEncoder.definitions(graphId, manifest).map(reparse)
  private val outputs  = commands.filter(_.hasDefineOutput).map(_.getDefineOutput)
  private val flows    = commands.filter(_.hasDefineFlow).map(_.getDefineFlow)

  private val externalManifest = PipelineManifest.fromGraph(
    PipelineGraph(
      Map(
        "main.bronze.orders" -> PipelineNode.ExternalTable("main.bronze.orders"),
        "gold" -> PipelineNode.MaterializedView("gold", "SELECT * FROM `main`.`bronze`.`orders`"),
      ),
      Set(DependencyEdge("main.bronze.orders", "gold")),
    )
  )

  def spec = suite("PipelineProtoEncoder (F8b)")(
    test("external tables produce no DefineOutput and no DefineFlow") {
      val cmds     = PipelineProtoEncoder.definitions(graphId, externalManifest).map(reparse)
      val outNames = cmds.filter(_.hasDefineOutput).map(_.getDefineOutput.getOutputName)
      val flowTgts = cmds.filter(_.hasDefineFlow).map(_.getDefineFlow.getTargetDatasetName)
      assertTrue(
        !outNames.contains("main.bronze.orders"), // external source: never registered
        outNames.contains("gold"),                // the managed view still is
        !flowTgts.contains("main.bronze.orders"), // nothing materializes the external
      )
    },
    test("source schema is emitted for file sources but suppressed for rate") {
      import dev.sdp.core.algebra.Rel
      val json = AlgebraProtoEncoder.relation(
        Rel.DataSource("json", Map("path" -> "/in"), streaming = true, schemaDdl = Some("id BIGINT, v STRING"))
      )
      val rate = AlgebraProtoEncoder.relation(
        Rel.DataSource("rate", Map.empty, streaming = true, schemaDdl = Some("timestamp TIMESTAMP, value BIGINT"))
      )
      assertTrue(
        // file source: the declared DDL reaches the wire verbatim
        json.getRead.getDataSource.getSchema == "id BIGINT, v STRING",
        // rate rejects a user schema → kept local only, never sent
        !rate.getRead.getDataSource.hasSchema,
      )
    },
    test("every command round-trips through canonical protobuf bytes") {
      val raw = PipelineProtoEncoder.definitions(graphId, manifest)
      assertTrue(raw.map(reparse) == raw)
    },
    test("node-type mapping covers all four PipelineNode cases") {
      val byName = outputs.map(o => o.getOutputName -> o.getOutputType).toMap
      assertTrue(
        byName("bronze") == sc.OutputType.TABLE,
        byName("silver") == sc.OutputType.TABLE, // streaming-ness lives in the flow
        byName("gold") == sc.OutputType.MATERIALIZED_VIEW,
        byName("tmp") == sc.OutputType.TEMPORARY_VIEW,
      )
    },
    test("the default (delta) provider is never put on the wire; views carry none") {
      // delta is SDP's implicit catalog default AND is unalterable, so sending
      // it on create is redundant and on re-run is fatal
      // (DELTA_CANNOT_CHANGE_PROVIDER). We mirror the official SDP: omit it.
      val byName = outputs.map(o => o.getOutputName -> o).toMap
      assertTrue(
        byName("bronze").getOutputType == sc.OutputType.TABLE,
        byName("silver").getOutputType == sc.OutputType.TABLE,
        !byName("bronze").hasTableDetails,
        !byName("silver").hasTableDetails,
        !byName("gold").hasTableDetails,
        !byName("tmp").hasTableDetails,
      )
    },
    test("a genuinely non-default table format is still emitted") {
      val customManifest = PipelineManifest.fromGraph(
        PipelineGraph(Map("ice" -> PipelineNode.Table("ice", "iceberg")), Set.empty)
      )
      val out = PipelineProtoEncoder
        .definitions(graphId, customManifest)
        .map(reparse)
        .filter(_.hasDefineOutput)
        .map(_.getDefineOutput)
        .head
      assertTrue(
        out.getOutputType == sc.OutputType.TABLE,
        out.hasTableDetails,
        out.getTableDetails.getFormat == "iceberg",
      )
    },
    test("a streaming table's lineage edge becomes a streaming named-table read flow") {
      val flow = flows.find(_.getFlowName == "silver").get
      val read = flow.getRelationFlowDetails.getRelation.getRead
      assertTrue(
        flow.getTargetDatasetName == "silver",
        read.getNamedTable.getUnparsedIdentifier == "bronze",
        read.getIsStreaming,
      )
    },
    test("materialized and temporary views carry their SQL as a single flow") {
      val gold = flows.find(_.getFlowName == "gold").get
      val tmp  = flows.find(_.getFlowName == "tmp").get
      assertTrue(
        gold.getRelationFlowDetails.getRelation.getSql.getQuery.startsWith("SELECT id, count(*)"),
        tmp.getRelationFlowDetails.getRelation.getSql.getQuery == "SELECT * FROM bronze",
      )
    },
    test("fan-in produces one deterministic flow per upstream") {
      val wideFlows = flows.filter(_.getTargetDatasetName == "wide")
      assertTrue(
        wideFlows.map(_.getFlowName) == List("wide__from__bronze", "wide__from__silver"),
        wideFlows.forall(_.getRelationFlowDetails.getRelation.getRead.getIsStreaming),
      )
    },
    test("every definition carries the graph id") {
      assertTrue(
        outputs.forall(_.getDataflowGraphId == graphId),
        flows.forall(_.getDataflowGraphId == graphId),
      )
    },
    test("encoding is deterministic: equal manifests yield identical bytes") {
      val a = PipelineProtoEncoder.definitions(graphId, manifest).map(_.toByteArray.toList)
      val b = PipelineProtoEncoder.definitions(graphId, manifest).map(_.toByteArray.toList)
      assertTrue(a == b)
    },
    test("createDataflowGraph and startRun(dry) build the bracketing commands") {
      val create = reparse(PipelineProtoEncoder.createDataflowGraph(Some("main"), Some("sales")))
      val run    = reparse(PipelineProtoEncoder.startRun(graphId, dry = true, storage = "file:///tmp/sdp"))
      assertTrue(
        create.getCreateDataflowGraph.getDefaultCatalog == "main",
        create.getCreateDataflowGraph.getDefaultDatabase == "sales",
        run.getStartRun.getDataflowGraphId == graphId,
        run.getStartRun.getDry,
        run.getStartRun.getStorage == "file:///tmp/sdp",
      )
    },
  )
