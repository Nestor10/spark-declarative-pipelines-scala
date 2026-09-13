package dev.sdp.connect

import java.util.UUID

import dev.sdp.app.{GraphValidation, ManifestAssembly}
import dev.sdp.core.GraphFragment
import dev.sdp.core.algebra.Rel
import dev.sdp.dsl.*
import zio.*
import zio.test.*

/** The other half of AUTO CDC: what the MERGE actually *does* to the data.
  *
  * [[AutoCdcE2eSpec]] proves everything up to the engine's capability gate and
  * then stops at it — on a stock Spark 4.2.0 distribution nothing implements
  * `SupportsRowLevelOperations`, so an AUTO CDC target cannot be materialized
  * and rows CDC-5 (merge semantics) / CDC-6 (re-run from checkpoint) had no
  * observable behavior to assert. That ceiling is real and still asserted
  * there; this suite is the OTHER side of the same measurement — the same
  * client, the same flow, against a server that DOES have a merge-capable
  * table format.
  *
  * That format is Iceberg, whose `SparkTable` implements the contract (read off
  * the jar) — but Iceberg has no Spark 4.2 release yet, so the server-side jar
  * is an Apache **snapshot**, pinned to one timestamped build. See
  * [[SparkConnectTestServer.Iceberg42]] for the pin and the reasoning; it is
  * fixture material only and appears in no `libraryDependencies`.
  *
  * Note what stays honest: the CLIENT sends no table format anywhere in here
  * (D13). The target becomes an Iceberg table purely because the server's
  * catalog default says so — which is exactly how a user would get one.
  *
  * Gated on `SDP_INTEGRATION`; the first run also downloads ~48 MB once.
  */
object IcebergAutoCdcE2eSpec extends ZIOSpecDefault:

  private val enabled =
    sys.env.contains("SDP_INTEGRATION") || java.lang.Boolean.getBoolean("sdp.integration")

  private val Catalog  = "spark_catalog"
  private val Database = "default"

  /** CDC-6 needs TWO runs sharing one pipeline storage root — that root is
    * where the flow's streaming checkpoint lives, so a fresh UUID per run would
    * quietly turn "resume" into "start over" and the re-run assertion would
    * prove nothing. One value, computed once, used by both runs. */
  private val RerunStorage = s"file:///tmp/sdp-e2e-rerun-${UUID.randomUUID()}"

  // ------------------------------------------------------------------ reading
  // Rows come back through [[ServerRead]] — the server formats them and hands
  // them over inside a `raise_error` message, because this client has no Arrow
  // decoder. Shared with FullRefreshE2eSpec; see that object for the reasoning.
  private def readRows(
      server: SparkConnectTestServer.Server,
      table: String,
      columns: List[String],
  ): ZIO[Any, Throwable, List[String]] =
    ServerRead.rows(server, table, columns)

  // ------------------------------------------------------------------ fixtures
  /** CDC-5's source. The two events for key 1 are inserted with the NEWER one
    * physically first: a last-row-wins implementation would leave 'alice', so
    * seeing 'alice2' is evidence that `sequence_by` — not arrival order —
    * decided the winner. Key 2 is upserted and then tombstoned; key 3 is a
    * plain insert that must survive untouched. */
  private val scd1Seed = List(
    "CREATE DATABASE IF NOT EXISTS bronze",
    "DROP TABLE IF EXISTS bronze.cdc_scd1",
    "CREATE TABLE bronze.cdc_scd1 (id INT, name STRING, op STRING, seq BIGINT) USING parquet",
    """INSERT INTO bronze.cdc_scd1 VALUES
      |  (1, 'alice2', 'UPSERT', 2),
      |  (1, 'alice',  'UPSERT', 1),
      |  (2, 'bob',    'UPSERT', 1),
      |  (2, 'bob',    'DELETE', 3),
      |  (3, 'carol',  'UPSERT', 1)""".stripMargin,
    "SELECT assert_true((SELECT count(*) FROM bronze.cdc_scd1) = 5, 'scd1 fixture rows')",
  )

  /** CDC-6's source — its own table, so appending to it cannot disturb CDC-5. */
  private val rerunSeed = List(
    "CREATE DATABASE IF NOT EXISTS bronze",
    "DROP TABLE IF EXISTS bronze.cdc_rerun",
    "CREATE TABLE bronze.cdc_rerun (id INT, name STRING, op STRING, seq BIGINT) USING parquet",
    """INSERT INTO bronze.cdc_rerun VALUES
      |  (1, 'alice', 'UPSERT', 1),
      |  (2, 'bob',   'UPSERT', 1)""".stripMargin,
  )

  /** The second wave: an update to an existing key and a brand-new key. */
  private val rerunAppend = List(
    """INSERT INTO bronze.cdc_rerun VALUES
      |  (1, 'alice3', 'UPSERT', 5),
      |  (4, 'dora',   'UPSERT', 1)""".stripMargin
  )

  private def cdcPipeline(target: String, source: String): List[GraphFragment] =
    List(
      externalTable(source),
      createStreamingTable(target),
      createAutoCdcFlow(
        target = target,
        source = source,
        keys = Seq(col("id")),
        sequenceBy = col("seq"),
        applyAsDeletes = Some(col("op") === lit("DELETE")),
        exceptColumnList = Seq(col("op")),
      ),
    )

  private def assemble(fragments: List[GraphFragment]) =
    ManifestAssembly
      .assemble(fragments)
      .provide(ManifestAssembly.live, GraphValidation.live)
      .mapError(errs => new RuntimeException(errs.map(_.describe).mkString("; ")))

  private def runPipeline(
      server: SparkConnectTestServer.Server,
      fragments: List[GraphFragment],
      storage: String,
  ): ZIO[Any, Throwable, Either[PipelinesRegistration.RegistrationError, String]] =
    assemble(fragments).flatMap { manifest =>
      ZIO.scoped {
        PipelinesRegistration
          .register(
            server.host,
            server.port,
            manifest,
            storage = storage,
            dry = false,
            defaultCatalog = Some(Catalog),
            defaultDatabase = Some(Database),
          )
          .flatMap(handle => handle.progress.runDrain.as(handle.graphId))
      }.either
    }

  private def schemaOf(server: SparkConnectTestServer.Server, table: String) =
    PlanAnalysis.analyzeSchema(
      server.host,
      server.port,
      AlgebraProtoEncoder.relation(Rel.NamedTable(table, streaming = false)),
    )

  def spec =
    val tests = suite("AUTO CDC SCD1 merge semantics against an Iceberg-flavored Spark 4.2.0 server")(
      test("the server really is Iceberg-flavored: a format-less CREATE TABLE lands as Iceberg") {
        // The precondition the whole suite rests on, asserted rather than
        // assumed — and asserted the way a user would hit it: no USING clause,
        // so `spark.sql.sources.default` decides. `<table>.history` is an
        // Iceberg metadata table; it resolves only if the table really is one.
        for
          server <- ZIO.service[SparkConnectTestServer.Server]
          _ <- CatalogSeeder.run(
            server.host,
            server.port,
            List(
              "DROP TABLE IF EXISTS default.ice_probe",
              "CREATE TABLE default.ice_probe (id INT)",
              "INSERT INTO default.ice_probe VALUES (1)",
              "SELECT assert_true((SELECT count(*) FROM default.ice_probe.history) >= 1, 'not an iceberg table')",
            ),
          )
        yield assertCompletes
      },
      test("seeding the CDC sources over Connect succeeds") {
        for
          server <- ZIO.service[SparkConnectTestServer.Server]
          _      <- CatalogSeeder.run(server.host, server.port, scd1Seed ++ rerunSeed)
        yield assertCompletes
      },
      test("CDC-5: the SCD1 merge lands — latest-by-sequence wins, the tombstoned key is gone") {
        // The first time this project has observed AUTO CDC *data*. Everything
        // the client sent is now visible in the result: the key, the sequencing
        // column, apply_as_deletes, and except_column_list.
        for
          server <- ZIO.service[SparkConnectTestServer.Server]
          result <- runPipeline(
            server,
            cdcPipeline("dim_scd1", "bronze.cdc_scd1"),
            s"file:///tmp/sdp-e2e-${UUID.randomUUID()}",
          )
          rows <- readRows(server, "dim_scd1", List("id", "name", "seq"))
        yield assertTrue(
          result.isRight,
          // key 1: the seq=2 event wins over the seq=1 event that arrived after it
          rows.contains("1,alice2,2"),
          // key 3: an ordinary insert survives
          rows.contains("3,carol,1"),
          // key 2: upserted then deleted — the tombstone wins
          !rows.exists(_.startsWith("2,")),
          rows.size == 2,
        ) ?? s"run = $result; target rows = $rows"
      },
      test("CDC-5: the auxiliary state table exists alongside the target") {
        // The merge is stateful: SDP maintains `__spark_autocdc_aux_state_<target>`
        // next to the target (v4.2.0 AutoCdcAuxiliaryTable.identifier). It is
        // created by the engine, never by us — its presence is the server-side
        // evidence that the SCD1 machinery, not a plain append, produced the
        // rows above.
        for
          server <- ZIO.service[SparkConnectTestServer.Server]
          schema <- schemaOf(server, "__spark_autocdc_aux_state_dim_scd1")
        yield assertTrue(schema.map(_.name).contains("id")) ?? s"aux state schema was $schema"
      },
      test("CDC-6: a re-run resumes the flow's checkpoint and merges only the new events") {
        // Two runs sharing ONE storage root (hence one checkpoint). Run 1
        // establishes state from two events; then two more events are appended
        // to the source and run 2 merges them ONTO that state.
        //
        // The data alone cannot separate "resumed" from "replayed everything" —
        // an SCD1 merge is idempotent, so a full replay converges on the same
        // rows. So the checkpoint itself is read: SDP puts a streaming flow's
        // checkpoint at `<storage>/_checkpoints/<catalog>/<db>/<table>/<flow>`
        // (v4.2.0 SystemMetadata.FlowSystemMetadata.flowCheckpointsDirOpt), and
        // the offset log there is the engine's own record of which micro-batches
        // ran. Two batches (0, 1) under ONE checkpoint directory is exactly what
        // a resumed query leaves behind; a restarted one would have written
        // batch 0 twice, in two directories.
        for
          server <- ZIO.service[SparkConnectTestServer.Server]
          first  <- runPipeline(server, cdcPipeline("dim_rerun", "bronze.cdc_rerun"), RerunStorage)
          after1 <- readRows(server, "dim_rerun", List("id", "name", "seq"))
          _      <- CatalogSeeder.run(server.host, server.port, rerunAppend)
          second <- runPipeline(server, cdcPipeline("dim_rerun", "bronze.cdc_rerun"), RerunStorage)
          after2 <- readRows(server, "dim_rerun", List("id", "name", "seq"))
          offsets <- readRows(
            server,
            // DISTINCT because an offset log file is several text lines
            s"(SELECT DISTINCT _metadata.file_path AS p FROM " +
              s"text.`$RerunStorage/_checkpoints/spark_catalog/default/dim_rerun/*/*/offsets/*`)",
            List("p"),
          )
          batches = offsets.map(p => p.substring(p.lastIndexOf('/') + 1)).sorted
          dirs    = offsets.map(p => p.substring(0, p.lastIndexOf("/offsets/"))).distinct
        yield assertTrue(
          first.isRight,
          after1.sorted == List("1,alice,1", "2,bob,1"),
          second.isRight,
          // advanced by the new events …
          after2.contains("1,alice3,5"),
          after2.contains("4,dora,1"),
          // … and the untouched key kept its run-1 state
          after2.contains("2,bob,1"),
          after2.size == 3,
          // … because run 2 was micro-batch 1 of the SAME streaming query
          batches == List("0", "1"),
          dirs.size == 1,
        ) ?? s"run1 = $first, after run1 = $after1; run2 = $second, after run2 = $after2; offsets = $offsets"
      },
    ).provideShared(SparkConnectTestServer.layerFor(SparkConnectTestServer.Iceberg42))
      @@ TestAspect.withLiveEnvironment
      @@ TestAspect.sequential
      @@ TestAspect.timeout(15.minutes)

    if enabled then tests
    else tests @@ TestAspect.ignore
