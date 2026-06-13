package dev.sdp.connect

import dev.sdp.core.{FlowDetails, PipelineManifest, PipelineNode}
import org.apache.spark.connect.proto as sc

/** Thrown when the manifest carries a construct the pinned wire client cannot
  * encode. AUTO CDC (`AutoCdcFlowDetails`) only exists in the Spark 4.2+
  * `pipelines.proto`; this build pins `spark-connect-common 4.1.2`, whose
  * generated classes have no such message. `validate`/`manifest` work offline
  * today; `run`/`dry-run` cannot register such a flow until the dep bumps. */
final class UnsupportedWireFeature(message: String) extends RuntimeException(message)

/** Pure translation from the canonical [[PipelineManifest]] to the Spark
  * Declarative Pipelines wire protocol (`spark/connect/pipelines.proto`).
  *
  * Message names and field semantics were read from the canonical proto in
  * the Spark source tree — see ROADMAP "Protobuf toolchain: decided". The
  * notable mapping facts:
  *
  *   - There is no streaming-table output type on the wire. Streaming-ness
  *     lives in the *flow*: `StreamingTable` encodes as `OutputType.TABLE`
  *     plus flows whose `Read` relations set `is_streaming = true`.
  *   - `MaterializedView`/`TemporaryView` carry their SQL as their single
  *     flow (`Relation.sql`); lineage *into* them is embodied by that SQL,
  *     so manifest edges targeting them produce no additional flows.
  *   - Until the typed algebra lands (Phase 6), `Table`/`StreamingTable`
  *     flows encode graph *shape*: one flow per lineage edge, reading the
  *     upstream via `Read.named_table`.
  *
  * Everything here is deterministic: the manifest is canonically sorted and
  * the encoder preserves that order, so equal manifests yield byte-identical
  * command sequences.
  */
object PipelineProtoEncoder:

  /** Step 1 of the registration sequence: ask the server for a graph.
    * `sqlConf` entries apply to every flow in the graph (e.g.
    * `pipelines.incompatibleViewCheck.enabled` — the server's own escape
    * hatch for streaming reads over batch views).
    */
  def createDataflowGraph(
      defaultCatalog: Option[String] = None,
      defaultDatabase: Option[String] = None,
      sqlConf: Map[String, String] = Map.empty,
  ): sc.PipelineCommand =
    val builder = sc.PipelineCommand.CreateDataflowGraph.newBuilder()
    defaultCatalog.foreach(builder.setDefaultCatalog)
    defaultDatabase.foreach(builder.setDefaultDatabase)
    sqlConf.toList.sortBy(_._1).foreach((k, v) => builder.putSqlConf(k, v))
    sc.PipelineCommand.newBuilder().setCreateDataflowGraph(builder).build()

  /** Step 2: every `DefineOutput` and `DefineFlow` for the manifest, in
    * deterministic order (outputs in manifest node order; then authored
    * flows in manifest flow order; then shape-derived flows for nodes
    * without authored ones).
    *
    * Authored flows (manifest v2) carry real relation trees and take
    * precedence: a node with at least one authored flow gets no
    * shape-derived `Read.named_table` flows — its lineage edges exist
    * because of the authored relation in the first place.
    */
  def definitions(graphId: String, manifest: PipelineManifest): List[sc.PipelineCommand] =
    // External tables are read from the catalog, never registered — they
    // produce no DefineOutput (and no flow); the server resolves them as
    // usedExternalInputs of the flows that read them.
    val outputs = manifest.nodes.flatMap(defineOutput(graphId, _))

    val authoredTargets = manifest.flows.map(_.target).toSet
    // Disjoint plan-id ranges per flow (10k apart — far above any real plan's
    // node count): ids must not collide across flows in one graph, see
    // AlgebraProtoEncoder.relation.
    val authored = manifest.flows.zipWithIndex.map { (flow, i) =>
      flow.details match
        case FlowDetails.WriteRelation(rel) =>
          flowCommand(
            graphId,
            flow.name,
            flow.target,
            AlgebraProtoEncoder.relation(rel, planIdBase = i.toLong * 10000L),
            flow.once,
          )
        case _: FlowDetails.AutoCdc =>
          autoCdcFlowCommand(graphId, flow)
    }
    val derived = manifest.nodes
      .filterNot(n => authoredTargets.contains(n.id))
      .flatMap(defineFlows(graphId, _, manifest))

    outputs ++ authored ++ derived

  /** Step 3: start the run. `dry = true` is server-side validation only —
    * the cheapest end-to-end correctness check a build can ask for.
    *
    * @param storage checkpoint/metadata root; the server requires an
    *                absolute URI with a scheme (`file://`, `s3a://`, ...)
    */
  def startRun(graphId: String, dry: Boolean, storage: String): sc.PipelineCommand =
    sc.PipelineCommand
      .newBuilder()
      .setStartRun(
        sc.PipelineCommand.StartRun
          .newBuilder()
          .setDataflowGraphId(graphId)
          .setDry(dry)
          .setStorage(storage)
      )
      .build()

  // ------------------------------------------------------------------
  // outputs
  // ------------------------------------------------------------------

  private def defineOutput(graphId: String, node: PipelineNode): Option[sc.PipelineCommand] =
    val builder = sc.PipelineCommand.DefineOutput
      .newBuilder()
      .setDataflowGraphId(graphId)
      .setOutputName(node.id)

    val configured = node match
      case PipelineNode.Table(_, format) =>
        Some(tableOutput(builder, format))
      case PipelineNode.StreamingTable(_, format) =>
        // No STREAMING_TABLE on the wire: TABLE + streaming flows.
        Some(tableOutput(builder, format))
      case PipelineNode.MaterializedView(_, _) =>
        Some(builder.setOutputType(sc.OutputType.MATERIALIZED_VIEW))
      case PipelineNode.TemporaryView(_, _) =>
        Some(builder.setOutputType(sc.OutputType.TEMPORARY_VIEW))
      case PipelineNode.ExternalTable(_) =>
        None // external/source table — not a managed output

    configured.map(b => sc.PipelineCommand.newBuilder().setDefineOutput(b).build())

  /** Delta is SDP's implicit default provider — the catalog supplies it when no
    * format is sent, so it must never go on the wire (this mirrors the official
    * SDP, whose `Table.format` is `Option` and is `None` unless a user writes an
    * explicit `USING`). Tracks the DSL's `DslMacros.DefaultFormat`. */
  private val DefaultTableFormat = "delta"

  /** Build a TABLE output, emitting `format` **only when it is a genuinely
    * non-default provider**.
    *
    * Why: SDP's `DatasetManager.materializeTable` re-applies every resolved
    * property on each re-run's `alterTable`, and a set `format` makes the
    * server add the reserved `provider` (`resolveTableProperties` →
    * `PROP_PROVIDER`). Delta's `AlterTableSetPropertiesDeltaCommand` rejects
    * `provider` in *any* ALTER **unconditionally** (decompiled: the check sees
    * only the key, never the value — so even an unchanged `delta`→`delta` set
    * throws `DELTA_CANNOT_CHANGE_PROVIDER`). The official SDP avoids this by
    * leaving `format = None` on the common path; we do the same. The server
    * reads the format by proto presence (`Option.when(tableDetails.hasFormat)`),
    * so not setting it leaves `table.format = None` → no `provider` → re-runs
    * succeed, and the table is still Delta via the catalog default. A genuinely
    * non-default format (parquet/iceberg) is still emitted — and carries the
    * same upstream re-run limitation an explicit `USING <fmt>` has in Python. */
  private def tableOutput(
      builder: sc.PipelineCommand.DefineOutput.Builder,
      format: String,
  ): sc.PipelineCommand.DefineOutput.Builder =
    val out = builder.setOutputType(sc.OutputType.TABLE)
    if format.nonEmpty && format != DefaultTableFormat then
      val _ = out.setTableDetails(
        sc.PipelineCommand.DefineOutput.TableDetails.newBuilder().setFormat(format)
      )
    out

  // ------------------------------------------------------------------
  // flows
  // ------------------------------------------------------------------

  private def defineFlows(
      graphId: String,
      node: PipelineNode,
      manifest: PipelineManifest,
  ): List[sc.PipelineCommand] =
    node match
      case PipelineNode.MaterializedView(id, sql) =>
        List(flowCommand(graphId, flowName = id, target = id, relation = sqlRelation(sql)))

      case PipelineNode.TemporaryView(id, sql) =>
        List(flowCommand(graphId, flowName = id, target = id, relation = sqlRelation(sql)))

      case PipelineNode.Table(id, _) =>
        // SDP semantics (learned from the live 4.1.2 analyzer): on the wire,
        // OutputType.TABLE IS a streaming table — batch relations can't feed
        // it directly, and DefineFlow.once (the proto's escape hatch) is
        // rejected by the 4.1.2 server (DEFINE_FLOW_ONCE_OPTION_NOT_SUPPORTED).
        // Shape-only batch reads are still emitted here so lineage round-trips;
        // semantically valid table feeds need real relations (STREAM(...) /
        // streaming sources) — that arrives with the F11 algebra.
        readFlows(graphId, id, manifest, streaming = false, once = false)

      case PipelineNode.StreamingTable(id, _) =>
        readFlows(graphId, id, manifest, streaming = true, once = false)

      case PipelineNode.ExternalTable(_) =>
        Nil // external/source tables have no flow — read from the catalog

  /** Shape-honest flows for lineage edges: one flow per upstream. A single
    * upstream uses the dataset's own name as the flow name (the implicit
    * default flow); fan-in disambiguates deterministically.
    */
  private def readFlows(
      graphId: String,
      target: String,
      manifest: PipelineManifest,
      streaming: Boolean,
      once: Boolean,
  ): List[sc.PipelineCommand] =
    val upstreams = manifest.edges.collect { case e if e.to == target => e.from }
    upstreams match
      case Nil => Nil
      case single :: Nil =>
        List(flowCommand(graphId, target, target, readRelation(single, streaming), once))
      case many =>
        many.map { up =>
          flowCommand(graphId, s"${target}__from__$up", target, readRelation(up, streaming), once)
        }

  private def flowCommand(
      graphId: String,
      flowName: String,
      target: String,
      relation: sc.Relation,
      once: Boolean = false,
  ): sc.PipelineCommand =
    val flow = sc.PipelineCommand.DefineFlow
      .newBuilder()
      .setDataflowGraphId(graphId)
      .setFlowName(flowName)
      .setTargetDatasetName(target)
      .setRelationFlowDetails(
        sc.PipelineCommand.DefineFlow.WriteRelationFlowDetails
          .newBuilder()
          .setRelation(relation)
      )
    if once then { val _ = flow.setOnce(true) }
    sc.PipelineCommand.newBuilder().setDefineFlow(flow).build()

  /** AUTO CDC flow → the wire. **Gated**: the pinned `spark-connect-common
    * 4.1.2` artifact has no `AutoCdcFlowDetails` message, so we cannot build the
    * `DefineFlow.auto_cdc_flow_details` oneof branch. Fail loud with a readable,
    * typed error rather than silently dropping the flow — `validate`/`manifest`
    * already accepted it offline, so the user only hits this at `run`/`dry-run`.
    *
    * GATE(spark-4.2): when the dep bumps to `spark-connect-common >= 4.2.0`,
    * delete the throw and emit the oneof branch. From `pipelines.proto`
    * v4.2.0-rc1 (`AutoCdcFlowDetails`, field numbers noted in [[FlowDetails]]):
    * {{{
    *   val cdc = flow.details.asInstanceOf[FlowDetails.AutoCdc]
    *   val ac  = sc.PipelineCommand.DefineFlow.AutoCdcFlowDetails.newBuilder()
    *   ac.setSource(cdc.source)                                    // field 1
    *   cdc.keys.foreach(k => ac.addKeys(AlgebraProtoEncoder.expression(k)))        // 2
    *   ac.setSequenceBy(AlgebraProtoEncoder.expression(cdc.sequenceBy))            // 3
    *   cdc.applyAsDeletes.foreach(e => ac.setApplyAsDeletes(AlgebraProtoEncoder.expression(e)))   // 6
    *   cdc.applyAsTruncates.foreach(e => ac.setApplyAsTruncates(AlgebraProtoEncoder.expression(e)))// 7
    *   cdc.columnList.foreach(e => ac.addColumnList(AlgebraProtoEncoder.expression(e)))            // 8
    *   cdc.exceptColumnList.foreach(e => ac.addExceptColumnList(AlgebraProtoEncoder.expression(e)))// 9
    *   cdc.scdType match { case ScdType.Scd1 => ac.setStoredAsScdType(sc.SCDType.SCD_TYPE_1) }     // 10
    *   cdc.ignoreNullUpdatesColumnList.foreach(e => ac.addIgnoreNullUpdatesColumnList(...))        // 14
    *   cdc.ignoreNullUpdatesExceptColumnList.foreach(e => ac.addIgnoreNullUpdatesExceptColumnList(...))// 15
    *   val df = sc.PipelineCommand.DefineFlow.newBuilder()
    *     .setDataflowGraphId(graphId).setFlowName(flow.name)
    *     .setTargetDatasetName(flow.target).setAutoCdcFlowDetails(ac)
    *   if flow.once then df.setOnce(true)
    *   sc.PipelineCommand.newBuilder().setDefineFlow(df).build()
    * }}}
    * (`AlgebraProtoEncoder.expression` is the `Ex` → `sc.Expression` encoder.)
    */
  private def autoCdcFlowCommand(graphId: String, flow: dev.sdp.core.Flow): sc.PipelineCommand =
    throw new UnsupportedWireFeature(
      s"AUTO CDC flow '${flow.name}' (target '${flow.target}') requires a Spark 4.2+ wire client " +
        "(spark-connect-common >= 4.2.0); this build pins 4.1.2 — validate/manifest work, " +
        "run/dry-run cannot register this flow yet."
    )

  private def readRelation(upstream: String, streaming: Boolean): sc.Relation =
    sc.Relation
      .newBuilder()
      .setRead(
        sc.Read
          .newBuilder()
          .setNamedTable(sc.Read.NamedTable.newBuilder().setUnparsedIdentifier(upstream))
          .setIsStreaming(streaming)
      )
      .build()

  private def sqlRelation(query: String): sc.Relation =
    sc.Relation
      .newBuilder()
      .setSql(sc.SQL.newBuilder().setQuery(query))
      .build()
