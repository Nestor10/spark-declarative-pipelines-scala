package dev.sdp.connect

import java.util.UUID

import dev.sdp.app.{GraphValidation, ManifestAssembly}
import dev.sdp.core.GraphFragment
import dev.sdp.dsl.*
import zio.*
import zio.test.*

/** The dependency-edge drop, kept as a live regression detector.
  *
  * The user's decision on this bug was: do not work around it, record it and
  * keep a detector. So this suite asserts what the server **currently does**,
  * including the wrong half — test (b) is written so that it goes RED on the
  * day Spark starts behaving, which is the only kind of alarm that survives a
  * fix landing upstream without anyone here noticing.
  *
  * The mechanism (v4.2.0 line numbers, read from `../spark` at the tag, and
  * reproduced independently with the image's own Python `spark-pipelines` CLI —
  * see `context/upstream/spark-sdp-eager-analysis-edge-drop.md`):
  *
  *   1. `PipelinesHandler.defineFlow` (`:391`) converts the proto relation to a
  *      `LogicalPlan` at DefineFlow RPC time and freezes it into the flow
  *      function.
  *   2. `SparkConnectPlanner.transformWithColumns` (`:1312`) eagerly analyses
  *      its child during that conversion — `Dataset.ofRows(session, child)`
  *      inside a `try`, falling back to `Project(UnresolvedStarWithColumns(…),
  *      child)` on `AnalysisException`.
  *   3. `FlowAnalysis.analyze` records an in-graph read ONLY by matching
  *      `case u: UnresolvedRelation` (`:118` streaming, `:129` batch).
  *
  * So on a CLEAN catalog the eager analysis fails, the catch fires, the
  * `UnresolvedRelation` survives, the edge is recorded and run 1 is correct. On
  * a re-run the upstream table exists, the eager analysis SUCCEEDS, the
  * `UnresolvedRelation` is consumed, no edge is recorded — and the downstream
  * flow is scheduled concurrently with its own upstream, reading the table
  * `DatasetManager` has just TRUNCATEd (FR-1). Zero rows, `Run is COMPLETED`,
  * exit 0.
  *
  * `transformProject` (`:1820`) has no eager analysis, which is why `select` is
  * the control: identical graph, identical read, identical output schema, one
  * operator different.
  *
  * Test (c) is a different bug in the same neighbourhood, kept here because the
  * fixture is the same shape: a read that mis-qualifies is not an error, it is
  * an EXTERNAL read of whatever catalog table happens to carry that name.
  *
  * Gated on `SDP_INTEGRATION`; stock `apache/spark:4.2.0`, no fixture jars.
  */
object EdgeDropProbeSpec extends ZIOSpecDefault:

  private val enabled =
    sys.env.contains("SDP_INTEGRATION") || java.lang.Boolean.getBoolean("sdp.integration")

  private val Catalog  = "spark_catalog"
  private val Database = "default"

  /** One storage root per test, fresh per suite run: these are materialized
    * views, so nothing here depends on checkpoint continuity — what has to be
    * continuous is the CATALOG, and that is what run 1 leaves behind for run 2. */
  private def storage(tag: String) = s"file:///tmp/sdp-it-edgedrop-$tag-${UUID.randomUUID()}"

  private def assemble(fragments: List[GraphFragment]) =
    ManifestAssembly
      .assemble(fragments)
      .provide(ManifestAssembly.live, GraphValidation.live)
      .mapError(errs => new RuntimeException(errs.map(_.describe).mkString("; ")))

  /** One run, with every progress event kept: the row counts are the verdict,
    * the events are the receipt for *why* — `Starting flow` lines interleaved
    * with the upstream's `COMPLETED` line is the concurrency itself. */
  private def runPipeline(
      server: SparkConnectTestServer.Server,
      fragments: List[GraphFragment],
      storageRoot: String,
      defaultDatabase: String = Database,
  ): ZIO[Any, Throwable, (Boolean, List[String])] =
    assemble(fragments).flatMap { manifest =>
      ZIO.scoped {
        PipelinesRegistration
          .register(
            server.host,
            server.port,
            manifest,
            storage = storageRoot,
            dry = false,
            defaultCatalog = Some(Catalog),
            defaultDatabase = Some(defaultDatabase),
          )
          .flatMap(_.progress.runCollect)
      }.either.map {
        case Right(events) => (true, events.map(_.raw).toList)
        case Left(err)     => (false, List(err.describe))
      }
    }

  /** Only the lines that carry ordering, flattened to one line each. */
  private def ordering(events: List[String]): List[String] =
    events.map(_.linesIterator.next()).filter(l => l.contains("Flow ") && !l.contains("QUEUED"))

  // ------------------------------------------------------------------ (a)/(b)
  // Identical graphs, identical read, identical output schema. The ONLY
  // difference between them is the operator stacked on the read, and therefore
  // which SparkConnectPlanner transform converts it.

  private def twoNodePipeline(suffix: String, wc: Boolean): List[GraphFragment] =
    val source = s"regions_$suffix"
    List(
      materializedView(source) {
        spark.createDataFrame(Seq((1, "north"), (2, "south"))).toDF("id", "name")
      },
      materializedView(s"enriched_$suffix") {
        val read = spark.read.table(source)
        if wc then read.withColumn("marker", functions.upper(col("name")))
        else read.select(col("id"), col("name"), functions.upper(col("name")).as("marker"))
      },
    )

  private def cleanTwoNode(suffix: String) = List(
    s"DROP TABLE IF EXISTS $Catalog.$Database.enriched_$suffix",
    s"DROP TABLE IF EXISTS $Catalog.$Database.regions_$suffix",
  )

  // ---------------------------------------------------------------------- (c)
  // The outputs are declared into `silver`, the graph default database is
  // `default`, and the read is BARE — so the read qualifies to
  // `spark_catalog.default.decoy_regions` while the graph holds
  // `spark_catalog.silver.decoy_regions`. A same-named table pre-seeded in the
  // default database is the decoy: if its row shows up downstream, the read was
  // classified EXTERNAL and no edge exists.
  //
  // `externalTable` is declared for the read because OUR offline validator
  // refuses an undeclared one as a dangling edge — that refusal is the only
  // thing standing between an author and this behavior, and declaring the read
  // external is exactly what an author does to silence it.

  private val decoyPipeline: List[GraphFragment] = List(
    externalTable("decoy_regions"),
    materializedView("silver.decoy_regions") {
      spark.createDataFrame(Seq((1, "north"), (2, "south"))).toDF("id", "name")
    },
    materializedView("silver.decoy_enriched") {
      spark.read.table("decoy_regions").select(col("id"), col("name"), lit("ENRICHED").as("marker"))
    },
  )

  private val cleanDecoy = List(
    s"CREATE NAMESPACE IF NOT EXISTS $Catalog.silver",
    s"DROP TABLE IF EXISTS $Catalog.silver.decoy_enriched",
    s"DROP TABLE IF EXISTS $Catalog.silver.decoy_regions",
    s"DROP TABLE IF EXISTS $Catalog.$Database.decoy_regions",
    s"CREATE TABLE $Catalog.$Database.decoy_regions (id INT, name STRING)",
    s"INSERT INTO $Catalog.$Database.decoy_regions VALUES (99, 'DECOY')",
  )

  def spec =
    val tests = suite("SDP dependency-edge drop on re-run (live Spark 4.2.0)")(
      test("select over an in-graph read keeps its edge on the re-run") {
        val suffix  = "sel"
        val root    = storage(suffix)
        val enrich  = s"$Catalog.$Database.enriched_$suffix"
        for
          server <- ZIO.service[SparkConnectTestServer.Server]
          _      <- CatalogSeeder.run(server.host, server.port, cleanTwoNode(suffix))
          (ok1, ev1) <- runPipeline(server, twoNodePipeline(suffix, wc = false), root)
          rows1      <- ServerRead.rows(server, enrich, List("id", "name", "marker"))
          (ok2, ev2) <- runPipeline(server, twoNodePipeline(suffix, wc = false), root)
          rows2      <- ServerRead.rows(server, enrich, List("id", "name", "marker"))
          _ = println(
            s"""|select (control), measured:
                |  run 1 rows: $rows1
                |  run 2 rows: $rows2
                |  run 2 ordering:
                |${ordering(ev2).map("    " + _).mkString("\n")}""".stripMargin
          )
        yield assertTrue(
          ok1,
          ok2,
          // the edge holds in BOTH directions of time: the control is only a
          // control if it is correct on the fresh run too
          rows1.size == 2,
          rows2.size == 2,
          rows2 == List("1,north,NORTH", "2,south,SOUTH"),
        ) ?? s"runs = ($ok1, $ok2); rows = $rows1 → $rows2; run 1 events = ${ev1.size}"
      },
      test(
        "documents SPARK-XXXXX: withColumn over an in-graph read loses its edge on re-run — " +
          "flip this assertion when the fix lands"
      ) {
        val suffix = "wc"
        val root   = storage(suffix)
        val enrich = s"$Catalog.$Database.enriched_$suffix"
        val src    = s"$Catalog.$Database.regions_$suffix"
        for
          server <- ZIO.service[SparkConnectTestServer.Server]
          _      <- CatalogSeeder.run(server.host, server.port, cleanTwoNode(suffix))
          (ok1, ev1) <- runPipeline(server, twoNodePipeline(suffix, wc = true), root)
          rows1      <- ServerRead.rows(server, enrich, List("id", "name", "marker"))
          (ok2, ev2) <- runPipeline(server, twoNodePipeline(suffix, wc = true), root)
          rows2      <- ServerRead.rows(server, enrich, List("id", "name", "marker"))
          upstream2  <- ServerRead.rows(server, src, List("id", "name"))
          _ = println(
            s"""|withColumn, measured:
                |  run 1 rows: $rows1   (clean catalog: eager analysis fails, the UnresolvedRelation survives, the edge is recorded)
                |  run 2 rows: $rows2   (upstream exists: eager analysis succeeds, no UnresolvedRelation, no edge)
                |  run 2 upstream rows: $upstream2  (the upstream itself is fine — the downstream read the TRUNCATEd table)
                |  run 1 ordering:
                |${ordering(ev1).map("    " + _).mkString("\n")}
                |  run 2 ordering:
                |${ordering(ev2).map("    " + _).mkString("\n")}""".stripMargin
          )
        yield assertTrue(
          // the run REPORTS success in both cases — that is the defect: nothing
          // on the wire distinguishes run 2 from run 1
          ok1,
          ok2,
          // run 1 is correct, which is what makes this survive every smoke test
          rows1.size == 2,
          // run 2: the edge is gone, the downstream ran concurrently with its
          // own upstream and read the table DatasetManager had just truncated
          rows2.isEmpty,
          // … and the upstream itself is intact, so this is ordering, not loss
          upstream2.size == 2,
        ) ?? s"runs = ($ok1, $ok2); rows = $rows1 → $rows2; upstream after run 2 = $upstream2"
      },
      test("a mis-qualified in-graph read is served as an external read of the same-named catalog table") {
        val root   = storage("decoy")
        val enrich = s"$Catalog.silver.decoy_enriched"
        for
          server <- ZIO.service[SparkConnectTestServer.Server]
          _      <- CatalogSeeder.run(server.host, server.port, cleanDecoy)
          (ok, ev) <- runPipeline(server, decoyPipeline, root)
          rows     <- ServerRead.rows(server, enrich, List("id", "name", "marker"))
          inGraph  <- ServerRead.rows(server, s"$Catalog.silver.decoy_regions", List("id", "name"))
          _ = println(
            s"""|mis-qualified read, measured:
                |  in-graph silver.decoy_regions : $inGraph
                |  downstream silver.decoy_enriched: $rows
                |  ordering:
                |${ordering(ev).map("    " + _).mkString("\n")}""".stripMargin
          )
        yield assertTrue(
          ok,
          // the in-graph table the author meant holds what they wrote
          inGraph == List("1,north", "2,south"),
          // and the downstream read something else entirely, on a FRESH run,
          // with no warning anywhere
          rows == List("99,DECOY,ENRICHED"),
        ) ?? s"run = $ok; in-graph = $inGraph; downstream = $rows"
      },
    ).provideShared(SparkConnectTestServer.layerFor(SparkConnectTestServer.Spark42Image))
      @@ TestAspect.withLiveEnvironment
      @@ TestAspect.sequential
      @@ TestAspect.timeout(15.minutes)

    if enabled then tests
    else tests @@ TestAspect.ignore
