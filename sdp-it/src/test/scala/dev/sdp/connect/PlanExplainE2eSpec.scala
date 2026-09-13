package dev.sdp.connect

import dev.sdp.app.{GraphValidation, ManifestAssembly}
import dev.sdp.core.{GraphFragment, PipelineManifest}
import dev.sdp.dsl.*
import zio.*
import zio.test.*

/** `sdpExplain` against a live Spark 4.2.0 server — the dependency-edge tell,
  * MEASURED rather than asserted from a fixture.
  *
  * `PlanDiagnosticsSpec` proves the heuristic reads recorded plan text
  * correctly. This proves the plan text is what we think it is: that a real
  * server, handed the bytes our encoder produces, renders a
  * `'UnresolvedRelation` for a select-shaped flow and a pre-resolved relation
  * for a `withColumn`-shaped one — and that the difference appears **only once
  * the upstream exists in the catalog**.
  *
  * That last clause is the whole upstream behaviour in one experiment.
  * `SparkConnectPlanner.transformWithColumns` eagerly analyzes its child while
  * DECODING the request, so the moment the upstream table is real, the read
  * resolves before the pipeline ever sees it and no pipeline dependency is
  * registered for it. Identical bytes, different catalog, different reading:
  *
  *   - **before** the upstream exists — both flows are refused with
  *     `TABLE_OR_VIEW_NOT_FOUND` (informative, not a failure: nothing can be
  *     pre-resolved before it exists);
  *   - **after** — the select flow still says `'UnresolvedRelation`, and the
  *     `withColumn` flow does not.
  *
  * Nothing is registered and nothing is run: an explain is `AnalyzePlan`, so
  * this suite needs no storage root, no checkpoints, and no pipeline execution.
  * Stock `apache/spark:4.2.0` with its parquet default is enough.
  *
  * Gated on `SDP_INTEGRATION`.
  */
object PlanExplainE2eSpec extends ZIOSpecDefault:

  private val enabled =
    sys.env.contains("SDP_INTEGRATION") || java.lang.Boolean.getBoolean("sdp.integration")

  private val Source = "bronze_pe"
  private val Mid    = "silver_pe"

  /** The catalog fixture: the pipeline's SOURCE exists from the start, the
    * in-graph `silver_pe` deliberately does not. */
  private val seed = List(
    s"DROP TABLE IF EXISTS $Mid",
    s"DROP TABLE IF EXISTS $Source",
    s"CREATE TABLE $Source (id INT, amount DOUBLE) USING parquet",
    s"INSERT INTO $Source VALUES (1, 10.0), (2, 20.0)",
  )

  /** …and then it does. This single statement is the independent variable:
    * the manifest, the bytes and the server are unchanged across it. */
  private val materializeMid =
    List(s"CREATE TABLE $Mid USING parquet AS SELECT id, amount FROM $Source")

  /** Two flows over the SAME in-graph read, differing only in the operator
    * above it — the A/B the classification has to separate. */
  private val pipeline: List[GraphFragment] = List(
    externalTable(Source),
    table(Mid) { spark.table(Source).select(col("id"), col("amount")) },
    table("gold_select") { spark.table(Mid).select(col("id")) },
    table("gold_withcolumn") { spark.table(Mid).withColumn("tag", lit(1)) },
  )

  private val manifest: ZIO[Any, Throwable, PipelineManifest] =
    ManifestAssembly
      .assemble(pipeline)
      .provide(ManifestAssembly.live, GraphValidation.live)
      .mapError(errs => new RuntimeException(errs.map(_.describe).mkString("; ")))

  private def explain(
      server: SparkConnectTestServer.Server,
      m: PipelineManifest,
  ): ZIO[Any, Throwable, PlanExplain.Report] =
    PlanExplain
      .explain(
        server.host,
        server.port,
        m,
        flowName = None,
        mode = PlanAnalysis.ExplainMode.Extended,
        defaultCatalog = Some("spark_catalog"),
        defaultDatabase = Some("default"),
      )
      .mapError(err => new RuntimeException(err.describe))

  private def verdictOf(report: PlanExplain.Report, flow: String): String =
    report.flows.find(_.flowName == flow).map(_.outcome) match
      case Some(PlanExplain.Outcome.Explained(_, reads)) =>
        reads.map(_.verdict).mkString(", ")
      case Some(PlanExplain.Outcome.Rejected(detail)) => s"REJECTED: ${detail.take(90)}"
      case Some(PlanExplain.Outcome.Skipped(reason))  => s"SKIPPED: $reason"
      case None                                       => "<flow missing>"

  private def isPreResolved(report: PlanExplain.Report, flow: String): Boolean =
    report.flows.find(_.flowName == flow).map(_.outcome).exists {
      case PlanExplain.Outcome.Explained(_, reads) =>
        reads.exists(_.verdict match
          case _: PlanDiagnostics.Verdict.PreResolved => true
          case _                                      => false
        )
      case _ => false
    }

  private def isUnresolved(report: PlanExplain.Report, flow: String): Boolean =
    report.flows.find(_.flowName == flow).map(_.outcome).exists {
      case PlanExplain.Outcome.Explained(_, reads) =>
        reads.nonEmpty && reads.forall(_.verdict == PlanDiagnostics.Verdict.Unresolved)
      case _ => false
    }

  private def isRejected(report: PlanExplain.Report, flow: String): Boolean =
    report.flows.find(_.flowName == flow).map(_.outcome).exists {
      case _: PlanExplain.Outcome.Rejected => true
      case _                               => false
    }

  def spec =
    val tests = suite("sdpExplain against a live Spark 4.2.0 server")(
      test("the classification FLIPS when the in-graph upstream starts existing") {
        for
          server <- ZIO.service[SparkConnectTestServer.Server]
          m      <- manifest
          _      <- CatalogSeeder.run(server.host, server.port, seed)

          // ---- before: the in-graph upstream does not exist yet
          before <- explain(server, m)

          // ---- the ONE change: the upstream becomes a real catalog table
          _     <- CatalogSeeder.run(server.host, server.port, materializeMid)
          after <- explain(server, m)

          // Printed, not merely asserted: these four lines ARE the measurement.
          _ = println(
            s"""|sdpExplain, measured against ${server.host}:${server.port}:
                |  before $Mid exists:
                |    gold_select     : ${verdictOf(before, "gold_select")}
                |    gold_withcolumn : ${verdictOf(before, "gold_withcolumn")}
                |  after $Mid exists:
                |    gold_select     : ${verdictOf(after, "gold_select")}
                |    gold_withcolumn : ${verdictOf(after, "gold_withcolumn")}
                |
                |${PlanExplain.render(after).mkString("\n")}""".stripMargin
          )
        yield assertTrue(
          // (a) before: nothing resolvable, so the server refuses both — and the
          // report carries that as information, not as a failed effect
          isRejected(before, "gold_select"),
          isRejected(before, "gold_withcolumn"),

          // (b) after: the select-shaped flow still arrives as a NAME — the
          // pipeline will see the read and register the dependency
          isUnresolved(after, "gold_select"),

          // (c) …and the withColumn-shaped flow does NOT: it was resolved
          // during decoding. This is the upstream behaviour, visible from the
          // outside — reported, not worked around.
          isPreResolved(after, "gold_withcolumn"),
        ) ?? (s"before: select=${verdictOf(before, "gold_select")}, " +
          s"withColumn=${verdictOf(before, "gold_withcolumn")}; " +
          s"after: select=${verdictOf(after, "gold_select")}, " +
          s"withColumn=${verdictOf(after, "gold_withcolumn")}")
      }
    ).provideShared(SparkConnectTestServer.layerFor(SparkConnectTestServer.Spark42Image))
      @@ TestAspect.withLiveEnvironment
      @@ TestAspect.sequential
      @@ TestAspect.timeout(10.minutes)

    if enabled then tests
    else tests @@ TestAspect.ignore
