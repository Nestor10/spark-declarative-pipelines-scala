package dev.sdp.connect

import java.util.UUID

import dev.sdp.app.{GraphValidation, ManifestAssembly}
import dev.sdp.core.GraphFragment
import dev.sdp.dsl.*
import zio.*
import zio.test.*

/** `StartRun.full_refresh_all` against a live server — the `sdpFullRefresh`
  * feature, measured rather than read.
  *
  * Everything this client can ask the server to do destructively is in ONE
  * flag, and until now nothing exercised it: matrix rows FR-2 (the checkpoint
  * roll) and the second half of CDC-6 (an AUTO CDC full refresh drops the
  * auxiliary state and replays) were both anchored to source only. Source is
  * the right authority for *what the server intends*; it is not evidence that
  * the released 4.2.0 binary does it, nor that our flag reaches the code that
  * does.
  *
  * The scenario is built so that **resume and rebuild produce different
  * answers** — which is the whole difficulty with full refresh, because an
  * idempotent flow converges on the same rows either way (that is exactly why
  * CDC-6's first half had to read the offset log). So the source SHRINKS before
  * the full refresh:
  *
  *   1. two ordinary runs accrue state: 3 keys in the target, micro-batches 0
  *      and 1 under checkpoint generation `0`;
  *   2. the source is overwritten with a single event;
  *   3. a full refresh runs.
  *
  * An incremental run at step 3 can only ever ADD rows — the target would keep
  * its 3 keys. A rebuild truncates and replays the current source, so the target
  * holds exactly 1. Three independent observables are asserted at once:
  *
  *   - **FR-2**: a NEW numbered checkpoint directory exists beside the old one,
  *     which is still there (`State.reset` creates `n+1`, it never deletes `n`),
  *     and the new generation's offset log starts again at batch 0;
  *   - **the rebuild**: the target's row count goes DOWN, to exactly the current
  *     source;
  *   - **CDC-6's second half**: the AUTO CDC auxiliary state table is a
  *     different table afterwards — its Iceberg history begins after the last
  *     snapshot of the one that existed before, which only a `DROP` + recreate
  *     can produce (`DatasetManager.materializeTable`, v4.2.0, drops it
  *     unconditionally for every fully-refreshed target).
  *
  * Iceberg-flavored, for the same reason [[IcebergAutoCdcE2eSpec]] is: nothing
  * in a stock Spark 4.2.0 distribution implements `SupportsRowLevelOperations`,
  * so an AUTO CDC target cannot be materialized at all without it — and the
  * `.history` metadata table is what makes "this is a different table now"
  * observable.
  *
  * NOT covered here: FR-4's silent demotion. It hinges on the
  * `pipelines.reset.allowed` TABLE property, which lives in the graph's table
  * spec (`DefineOutput.TableDetails.table_properties`) and which this client
  * deliberately never sends (D13) — there is no client surface to set it from,
  * so the row stays honestly Uncovered.
  *
  * Gated on `SDP_INTEGRATION`; shares the ~48 MB Iceberg fixture jar cache with
  * the AUTO CDC suite.
  */
object FullRefreshE2eSpec extends ZIOSpecDefault:

  private val enabled =
    sys.env.contains("SDP_INTEGRATION") || java.lang.Boolean.getBoolean("sdp.integration")

  private val Catalog  = "spark_catalog"
  private val Database = "default"

  /** ONE storage root for the whole scenario — it is where the checkpoint
    * lives, so a fresh UUID per run would turn every run into a first run and
    * the roll being measured would never happen. */
  private val Storage = s"file:///tmp/sdp-e2e-fullrefresh-${UUID.randomUUID()}"

  private val Source = "bronze.fr_cdc"
  private val Target = "dim_fullrefresh"

  /** SDP's own name for the AUTO CDC state store, next to the target
    * (v4.2.0 `AutoCdcAuxiliaryTable.identifier`).
    *
    * QUALIFIED with the database on purpose: an Iceberg metadata table is
    * addressed by appending a segment (`…<table>.history`), so an unqualified
    * name would parse as `<database>.<table>` and the server would look for a
    * *database* called `__spark_autocdc_aux_state_…` (measured — it answers
    * TABLE_OR_VIEW_NOT_FOUND, which reads like "the aux table is missing" and
    * is not). */
  private val Aux = s"$Database.__spark_autocdc_aux_state_$Target"

  private val seed = List(
    "CREATE DATABASE IF NOT EXISTS bronze",
    s"DROP TABLE IF EXISTS $Source",
    s"CREATE TABLE $Source (id INT, name STRING, op STRING, seq BIGINT) USING parquet",
    s"""INSERT INTO $Source VALUES
       |  (1, 'alice', 'UPSERT', 1),
       |  (2, 'bob',   'UPSERT', 1)""".stripMargin,
    s"DROP TABLE IF EXISTS $Target",
    s"DROP TABLE IF EXISTS $Aux",
  )

  /** Wave two: a third key, so run 2 has something to do and the offset log
    * advances to batch 1 under the SAME checkpoint generation. */
  private val wave2 = List(s"INSERT INTO $Source VALUES (3, 'carol', 'UPSERT', 1)")

  /** The source SHRINKS: one event, replacing all three. `INSERT OVERWRITE`
    * rather than drop-and-recreate, so the table identity (and therefore the
    * graph) is untouched and only the DATA is smaller. */
  private val shrink = List(s"INSERT OVERWRITE $Source VALUES (1, 'alice', 'UPSERT', 1)")

  private val pipeline: List[GraphFragment] = List(
    externalTable(Source),
    createStreamingTable(Target),
    createAutoCdcFlow(
      target = Target,
      source = Source,
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

  /** Register + run against the shared storage root. `fullRefresh` is the one
    * thing that varies — the same manifest, the same storage, one flag. */
  private def runPipeline(
      server: SparkConnectTestServer.Server,
      fullRefresh: Boolean,
  ): ZIO[Any, Throwable, Either[PipelinesRegistration.RegistrationError, String]] =
    assemble(pipeline).flatMap { manifest =>
      ZIO.scoped {
        PipelinesRegistration
          .register(
            server.host,
            server.port,
            manifest,
            storage = Storage,
            dry = false,
            fullRefresh = fullRefresh,
            defaultCatalog = Some(Catalog),
            defaultDatabase = Some(Database),
          )
          .flatMap(handle => handle.progress.runDrain.as(handle.graphId))
      }.either
    }

  /** Every offset-log file the target's flow has written, across ALL checkpoint
    * generations: `<storage>/_checkpoints/<catalog>/<db>/<table>/<flow>/<n>/offsets/<batch>`
    * (v4.2.0 `SystemMetadata.FlowSystemMetadata.flowCheckpointsDirOpt`). Read as
    * text through the server, because the files are inside the container.
    * DISTINCT because one offset file is several lines. */
  private def offsetFiles(server: SparkConnectTestServer.Server) =
    ServerRead.rows(
      server,
      s"(SELECT DISTINCT _metadata.file_path AS p FROM " +
        s"text.`$Storage/_checkpoints/$Catalog/$Database/$Target/*/*/offsets/*`)",
      List("p"),
    )

  /** `<generation>/<batch>` for each offset file — the pair that says which
    * checkpoint directory a micro-batch belongs to. */
  private def generations(paths: List[String]): List[(String, String)] =
    paths.map { p =>
      val batch = p.substring(p.lastIndexOf('/') + 1)
      val dir   = p.substring(0, p.lastIndexOf("/offsets/"))
      (dir.substring(dir.lastIndexOf('/') + 1), batch)
    }.distinct.sorted

  /** The newest / oldest snapshot timestamp of an Iceberg table, as a sortable
    * string. A table that was DROPped and recreated has a history that begins
    * after the previous one ended — the cheapest server-observable proof of a
    * new table generation that does not need us to tag anything. */
  private def historyBound(
      server: SparkConnectTestServer.Server,
      table: String,
      agg: String,
  ) =
    ServerRead
      .rows(server, s"(SELECT CAST($agg(made_current_at) AS STRING) AS t FROM $table.history)", List("t"))
      .map(_.headOption.getOrElse(""))

  def spec =
    val tests = suite("full refresh (StartRun.full_refresh_all) against a live Spark 4.2.0 server")(
      test("FR-2 + CDC-6: a full refresh rolls the checkpoint, rebuilds the target, and drops the AUTO CDC aux state") {
        for
          server <- ZIO.service[SparkConnectTestServer.Server]
          _      <- CatalogSeeder.run(server.host, server.port, seed)

          // ---- two ordinary runs: state accrues under checkpoint generation 0
          run1   <- runPipeline(server, fullRefresh = false)
          after1 <- ServerRead.rows(server, Target, List("id", "name", "seq"))
          _      <- CatalogSeeder.run(server.host, server.port, wave2)
          run2   <- runPipeline(server, fullRefresh = false)
          after2 <- ServerRead.rows(server, Target, List("id", "name", "seq"))
          before <- offsetFiles(server).map(generations)
          auxEnd <- historyBound(server, Aux, "max")

          // ---- the source shrinks, then the button is pressed
          _      <- CatalogSeeder.run(server.host, server.port, shrink)
          run3   <- runPipeline(server, fullRefresh = true)
          after3 <- ServerRead.rows(server, Target, List("id", "name", "seq"))
          rolled <- offsetFiles(server).map(generations)
          auxStart <- historyBound(server, Aux, "min")

          // Printed, not just attached to a failure: these four lines ARE the
          // measurement, and a green run that shows nothing is how a suite
          // quietly stops meaning anything.
          _ = println(
            s"""|full refresh, measured:
                |  target rows      : ${after1.size} → ${after2.size} → ${after3.size}  (source shrank to 1 before the refresh)
                |  target after     : $after3
                |  checkpoints before: ${before.map((g, b) => s"gen $g/batch $b").mkString(", ")}
                |  checkpoints after : ${rolled.map((g, b) => s"gen $g/batch $b").mkString(", ")}
                |  aux state history : previous generation ended $auxEnd, current begins $auxStart""".stripMargin
          )
        yield assertTrue(
          // (a) the full-refresh run COMPLETES, like any other run
          run1.isRight,
          run2.isRight,
          run3.isRight,
          // the two ordinary runs accrued: 2 keys, then 3
          after1.size == 2,
          after2.size == 3,
          // … as micro-batches 0 and 1 of ONE streaming query (CDC-6's first half)
          before == List(("0", "0"), ("0", "1")),

          // (b) FR-2: the checkpoint ROLLED to a new numbered sibling, and the
          // old generation is STILL THERE — reset creates n+1, it never deletes
          rolled.map(_._1).distinct == List("0", "1"),
          rolled.contains(("0", "0")),
          rolled.contains(("0", "1")),
          // the new generation starts over at batch 0: a fresh query, not a resume
          rolled.contains(("1", "0")),

          // (c) the target was REBUILT from the current source, not resumed:
          // an incremental run can only add rows, so 3 → 1 is only reachable
          // through TRUNCATE + replay
          after3.size == 1,
          after3 == List("1,alice,1"),
          after3.size < after2.size,

          // (d) CDC-6's second half: the auxiliary state table is a DIFFERENT
          // table now — its history begins after the previous one's last
          // snapshot, which only DROP + recreate produces
          auxEnd.nonEmpty,
          auxStart.nonEmpty,
          auxStart > auxEnd,
        ) ?? (s"runs = ($run1, $run2, $run3); rows = $after1 → $after2 → $after3; " +
          s"checkpoints $before → $rolled; aux history: before ended $auxEnd, after begins $auxStart")
      }
    ).provideShared(SparkConnectTestServer.layerFor(SparkConnectTestServer.Iceberg42))
      @@ TestAspect.withLiveEnvironment
      @@ TestAspect.sequential
      @@ TestAspect.timeout(15.minutes)

    if enabled then tests
    else tests @@ TestAspect.ignore
