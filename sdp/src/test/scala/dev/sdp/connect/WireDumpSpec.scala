package dev.sdp.connect

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

import scala.jdk.CollectionConverters.*

import com.google.protobuf.TextFormat
import dev.sdp.core.*
import dev.sdp.core.algebra.{Ex, Rel}
import org.apache.spark.connect.proto as sc
import zio.*
import zio.test.*

/** The offline half of the plan diagnostics: `sdpDumpWire` / `sdp dump-wire`.
  *
  * Three laws, and they are the reason the dump is worth attaching to a bug
  * report at all:
  *
  *   1. **Determinism** — two dumps of the same manifest are byte-identical.
  *      A diff between two dumps must mean the pipeline changed, never that
  *      the renderer felt different today (which is exactly what protobuf's
  *      *debug* printer does on purpose: redaction plus perturbed whitespace).
  *   2. **Completeness** — every flow and every output the encoder produces
  *      appears, in send order, once.
  *   3. **Round-trip** — `TextFormat.merge` parses each file back into the
  *      message it was rendered from. That is what makes the dump a *replayable
  *      artifact* rather than a pretty-printed log line, and it is also the
  *      test that keeps the `#` comment header honest.
  */
object WireDumpSpec extends ZIOSpecDefault:

  /** A graph with a MaterializedView (SQL flow), two authored relation flows
    * and an external read — enough shapes that a missing branch shows up. */
  private val manifest: PipelineManifest =
    PipelineManifest.fromGraphAndFlows(
      PipelineGraph(
        Map(
          "bronze.orders" -> PipelineNode.ExternalTable("bronze.orders"),
          "silver"        -> PipelineNode.StreamingTable("silver", "delta"),
          "gold"          -> PipelineNode.Table("gold", "parquet"),
          "daily"         -> PipelineNode.MaterializedView("daily", "SELECT * FROM gold"),
        ),
        Set(
          DependencyEdge("bronze.orders", "silver"),
          DependencyEdge("silver", "gold"),
          DependencyEdge("gold", "daily"),
        ),
      ),
      List(
        Flow(
          "silver",
          "silver",
          FlowDetails.WriteRelation(Rel.NamedTable("bronze.orders", streaming = true)),
        ),
        Flow(
          "gold",
          "gold",
          FlowDetails.WriteRelation(
            Rel.Project(Rel.NamedTable("silver", streaming = false), List(Ex.Star(None)))
          ),
        ),
      ),
    )

  private val options = WireDump.Options(
    defaultCatalog = Some("warehouse"),
    defaultDatabase = Some("dev_eric"),
    sqlConf = Map("pipelines.incompatibleViewCheck.enabled" -> "false", "a.b" -> "1"),
    storage = "file:///tmp/sdp/spec",
    dry = true,
  )

  def spec = suite("WireDump — the registration sequence as prototext")(
    test("determinism: two dumps of the same manifest are byte-identical") {
      val a = WireDump.entries(manifest, options)
      val b = WireDump.entries(manifest, options)
      assertTrue(
        a.map(_.fileName) == b.map(_.fileName),
        a.map(_.prototext) == b.map(_.prototext),
      )
    },
    test("the sequence is CreateDataflowGraph, then the definitions, then StartRun") {
      val names = WireDump.entries(manifest, options).map(_.fileName)
      assertTrue(
        names.head == "00-create-dataflow-graph.txtpb",
        names.last == "99-start-run.txtpb",
        // sorted order == send order: the numbering is what makes an `ls` readable
        names == names.sorted,
        names.forall(_.endsWith(".txtpb")),
      )
    },
    test("every flow the encoder emits has its own file, named after the flow") {
      val dump      = WireDump.entries(manifest, options)
      val flowFiles = dump.map(_.fileName).filter(_.contains("define-flow"))
      // Two authored flows (silver, gold) + the MV's SQL flow (daily).
      assertTrue(
        flowFiles.exists(_.endsWith("define-flow-silver.txtpb")),
        flowFiles.exists(_.endsWith("define-flow-gold.txtpb")),
        flowFiles.exists(_.endsWith("define-flow-daily.txtpb")),
        flowFiles.size == 3,
        // and one DefineOutput per managed node — the external table is NOT one
        dump.map(_.fileName).count(_.contains("define-output")) == 3,
      )
    },
    test("the dump is the ENCODER's output — the same commands the registration sends") {
      val dump = WireDump.entries(manifest, options)
      val live =
        PipelineProtoEncoder.createDataflowGraph(
          options.defaultCatalog,
          options.defaultDatabase,
          options.sqlConf,
        ) ::
          PipelineProtoEncoder.definitions(WireDump.GraphIdPlaceholder, manifest) :::
          List(
            PipelineProtoEncoder.startRun(WireDump.GraphIdPlaceholder, options.dry, options.storage)
          )
      assertTrue(dump.map(_.command) == live)
    },
    test("round-trip: TextFormat.merge parses every file back into the same message") {
      val dump = WireDump.entries(manifest, options)
      val reparsed = dump.map { e =>
        val builder = sc.PipelineCommand.newBuilder()
        TextFormat.merge(e.prototext, builder)
        builder.build()
      }
      assertTrue(reparsed == dump.map(_.command))
    },
    test("the graph id is the placeholder, never a fresh UUID") {
      val dump = WireDump.entries(manifest, options)
      val ids = dump.map(_.command).collect {
        case c if c.hasDefineFlow   => c.getDefineFlow.getDataflowGraphId
        case c if c.hasDefineOutput => c.getDefineOutput.getDataflowGraphId
        case c if c.hasStartRun     => c.getStartRun.getDataflowGraphId
      }
      assertTrue(ids.nonEmpty, ids.forall(_ == WireDump.GraphIdPlaceholder))
    },
    test("writeTo lands one file per entry and sweeps a previous dump's stale files") {
      // Everything that touches the filesystem is forced into plain values
      // BEFORE the temp directory is cleaned up: zio-test's smart assertions
      // evaluate lazily, so an `assertTrue(Files.exists(…))` would run after
      // the `finally` and report a file we deleted ourselves.
      ZIO
        .attemptBlocking {
          val dir = Files.createTempDirectory("sdp-wire-spec")
          try
            Files.write(dir.resolve("42-define-flow-ghost.txtpb"), "stale".getBytes(UTF_8))
            Files.write(dir.resolve("notes.md"), "mine".getBytes(UTF_8))

            val dump      = WireDump.entries(manifest, options)
            val written   = WireDump.writeTo(dir, dump)
            val allExist  = written.forall(Files.exists(_))
            val listing   = Files.list(dir)
            val onDisk =
              try listing.iterator.asScala.map(_.getFileName.toString).toSet
              finally listing.close()
            val firstFile = new String(Files.readAllBytes(written.head), UTF_8)
            (dump.size, written.size, allExist, onDisk, firstFile, dump.head.prototext)
          finally
            val leftovers = Files.list(dir)
            try leftovers.iterator.asScala.toList.foreach(Files.deleteIfExists(_))
            finally leftovers.close()
            val _ = Files.deleteIfExists(dir)
        }
        .map { (expected, actual, allExist, onDisk, firstFile, firstText) =>
          assertTrue(
            actual == expected,
            allExist,
            // the stale command file is gone; a file we did not write is untouched
            !onDisk.contains("42-define-flow-ghost.txtpb"),
            onDisk.contains("notes.md"),
            onDisk.contains("00-create-dataflow-graph.txtpb"),
            firstFile == firstText,
          )
        }
    },
    test("a name that is not file-system safe is slugged, and the command still names it") {
      val odd = PipelineManifest.fromGraphAndFlows(
        PipelineGraph(Map("a/b c" -> PipelineNode.Table("a/b c", "delta")), Set.empty),
        List(Flow("a/b c", "a/b c", FlowDetails.WriteRelation(Rel.Range(0, 1, 1)))),
      )
      val dump = WireDump.entries(odd, WireDump.Options())
      assertTrue(
        dump.exists(_.fileName == "01-define-output-a_b_c.txtpb"),
        dump.exists(_.fileName == "02-define-flow-a_b_c.txtpb"),
        // the SLUG is cosmetic: the wire still carries the author's real name
        dump.exists(e => e.command.hasDefineFlow && e.command.getDefineFlow.getFlowName == "a/b c"),
      )
    },
  )
