package dev.sdp.plugin.connect

import dev.sdp.core.*
import zio.*
import zio.test.*

/** F9a end-to-end: register a real pipeline graph against a live Spark 4.1.2
  * Connect server (container) and dry-run it — server-side validation of
  * everything this project produces, from manifest to wire.
  *
  * Gated: runs only when `SDP_INTEGRATION` (env) or `sdp.integration`
  * (system property) is set — CI without a container engine skips visibly,
  * it does not silently pass.
  */
object PipelinesRegistrationIntegrationSpec extends ZIOSpecDefault:

  private val enabled =
    sys.env.contains("SDP_INTEGRATION") || java.lang.Boolean.getBoolean("sdp.integration")

  /** Self-contained, *semantically* valid graph for the 4.1.2 analyzer:
    * a temporary view defined by real SQL, and a materialized view computed
    * from it. (Iterating against the live server taught us the SDP rules:
    * outputs of type TABLE are streaming tables — they cannot be fed by
    * batch relations, and `DefineFlow.once` is rejected by this server
    * version. Batch-derived datasets are materialized views.)
    */
  private val manifest = PipelineManifest.fromGraph(
    PipelineGraph(
      Map(
        "base_numbers" -> PipelineNode.TemporaryView("base_numbers", "SELECT id FROM range(1, 100)"),
        "doubled"      -> PipelineNode.MaterializedView("doubled", "SELECT id * 2 AS double_id FROM base_numbers"),
      ),
      Set(DependencyEdge("base_numbers", "doubled")),
    )
  )

  def spec =
    val tests = suite("PipelinesRegistration against a live server (F9a)")(
      test("registers the graph and the server assigns an id; dry-run validates") {
        for
          server  <- ZIO.service[SparkConnectTestServer.Server]
          graphId <- ZIO.scoped {
            PipelinesRegistration
              .register(server.host, server.port, manifest)
              .flatMap(h => h.progress.runDrain.as(h.graphId))
          }
        yield assertTrue(graphId.nonEmpty)
      },
      test("a pipeline authored entirely in the fluent flow language passes dry-run (F11c)") {
        import dev.sdp.app.{GraphValidation, ManifestAssembly}
        import dev.sdp.dsl.*

        // Exactly what an author writes: streaming leaf, streaming
        // transformation, batch aggregation — zero SQL strings.
        val silver = streamingTableFrom("silver_rates") {
          stream.source("rate").option("rowsPerSecond", "1").select(col("value"))
        }
        val gold = streamingTableFrom("gold_rates") {
          val positive = stream.table("silver_rates").where(col("value") > lit(0L))
          positive.select(col("value"), (col("value") % lit(10L)).as("bucket"))
        }
        val daily = materializedViewFrom("value_counts") {
          read.table("gold_rates").groupBy(col("value")).agg(fn("count", col("value")).as("n"))
        }

        for
          server   <- ZIO.service[SparkConnectTestServer.Server]
          manifest <- ManifestAssembly
            .assemble(List(silver, gold, daily))
            .provide(ManifestAssembly.live, GraphValidation.live)
            .mapError(errors => new RuntimeException(errors.map(_.describe).mkString("; ")))
          graphId <- ZIO.scoped {
            PipelinesRegistration
              .register(server.host, server.port, manifest)
              .flatMap(h => h.progress.runDrain.as(h.graphId))
          }
        yield assertTrue(
          graphId.nonEmpty,
          manifest.flows.size == 3,
          // lineage derived from the flow bodies themselves
          manifest.edges == List(
            DependencyEdge("gold_rates", "value_counts"),
            DependencyEdge("silver_rates", "gold_rates"),
          ),
        )
      },
      test("a manifest referencing a missing upstream is rejected by the server, typed") {
        val dangling = PipelineManifest.fromGraph(
          PipelineGraph(
            Map("lonely" -> PipelineNode.StreamingTable("lonely", "parquet")),
            Set(DependencyEdge("ghost_table", "lonely")),
          )
        )
        for
          server <- ZIO.service[SparkConnectTestServer.Server]
          exit <- ZIO.scoped {
            PipelinesRegistration
              .register(server.host, server.port, dangling)
              .flatMap(_.progress.runDrain)
          }.exit
        yield assertTrue(
          exit.causeOption.flatMap(_.failureOption).exists {
            case PipelinesRegistration.RegistrationError.ServerRejected(_) => true
            case _                                                         => false
          }
        )
      },
    ).provideShared(SparkConnectTestServer.layer) @@ TestAspect.withLiveEnvironment
      @@ TestAspect.sequential @@ TestAspect.timeout(5.minutes)

    if enabled then tests
    else tests @@ TestAspect.ignore
