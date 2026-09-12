package dev.sdp.connect

import java.util.UUID

import dev.sdp.app.{GraphValidation, ManifestAssembly}
import dev.sdp.core.GraphFragment
import dev.sdp.core.algebra.Rel
import dev.sdp.dsl.*
import org.apache.spark.connect.proto as sc
import zio.*
import zio.test.*

/** S1 end-to-end: AUTO CDC (SCD type 1) against a **released Spark 4.2.0**
  * Connect server — the first server generation that speaks
  * `DefineFlow.auto_cdc_flow_details`.
  *
  * Scenario modelled on upstream's `AutoCdcScd1SinglePipelineSuite`: a CDC
  * source carrying upsert/delete events with an `op` control column and a
  * `seq` sequencing column, a body-less streaming-table target, and one
  * `createAutoCdcFlow` keyed by `id` and ordered by `seq`.
  *
  * **What this suite can and cannot prove, measured not assumed (2026-09-12).**
  * AUTO CDC executes as a DSv2 `MERGE` (`Scd1BatchProcessor.mergeMicrobatchOnto
  * Target` → `DataFrame.mergeInto`), and `FlowExecution
  * .requireDestinationSupportsRowLevelOps` refuses the flow up front unless the
  * target's V2 table implements `SupportsRowLevelOperations`. **No table format
  * available for Spark 4.2 today satisfies that**, which was established by
  * running it, not by reading:
  *
  *   - stock catalog default (parquet, V1 file source) →
  *     `AUTOCDC_TARGET_DOES_NOT_SUPPORT_MERGE … (format: parquet)`;
  *   - Delta 4.4.0 (the first Delta built against Spark 4.2), loaded with
  *     `--packages` + `DeltaCatalog` + `spark.sql.sources.default=delta` → the
  *     **same** refusal, `(format: delta)`. Cause, from the jar itself:
  *     `DeltaTableV2 implements Table, SupportsWrite, V2TableWithV1Fallback` —
  *     Delta does MERGE through its own analyzer rules, not the DSv2 row-level
  *     contract SDP checks for;
  *   - Iceberg (whose `SparkTable` does implement it) has no Spark 4.2 build —
  *     Central carries `iceberg-spark-runtime-4.0/4.1` only.
  *
  * Upstream's own suites sidestep all of this with
  * `InMemoryRowLevelOperationTableCatalog`, a *test-jar* class absent from the
  * distribution (verified: no jar in the image contains it). So the container
  * stays the plain image — Delta buys nothing here.
  *
  * So the merge *semantics* (upserts in sequence order, deletes applied,
  * checkpoint-resumed re-run) are NOT assertable here, and are honestly left
  * uncovered in [[conformance.BehaviorInventory]] (rows CDC-5/CDC-6) rather
  * than faked. What IS asserted is everything up to and including the engine's
  * capability gate: the handshake, the registration of a real AUTO CDC flow,
  * server-side dry-run validation, the *materialized target schema* (which
  * proves `except_column_list` reached the engine and that SDP appended the
  * reserved CDC metadata column), the engine's refusal of a non-MERGE target,
  * and the two server contracts a client can trip over — non-identifier keys
  * and `DefineFlow.once`.
  *
  * Gated on `SDP_INTEGRATION` like every container suite; the 4.2.0 image is a
  * separate ~1.3 GB pull (see `docs/developing.md`).
  */
object AutoCdcE2eSpec extends ZIOSpecDefault:

  private val enabled =
    sys.env.contains("SDP_INTEGRATION") || java.lang.Boolean.getBoolean("sdp.integration")

  // The session catalog + database, sent explicitly on CreateDataflowGraph:
  // omitting them puts the server on its session fallback, which is where the
  // silent edge-drop of DEF-1/DEF-3 lives.
  private val Catalog  = "spark_catalog"
  private val Database = "default"

  /** The CDC source: two keys, an upsert-then-newer-upsert for `1`, and a
    * delete for `2` — the shape upstream's SCD1 smoke tests use. */
  private val seedStatements = List(
    "CREATE DATABASE IF NOT EXISTS bronze",
    "DROP TABLE IF EXISTS bronze.customer_cdc",
    "CREATE TABLE bronze.customer_cdc (id INT, name STRING, op STRING, seq BIGINT) USING parquet",
    """INSERT INTO bronze.customer_cdc VALUES
      |  (1, 'alice',  'UPSERT', 1),
      |  (2, 'bob',    'UPSERT', 1),
      |  (1, 'alice2', 'UPSERT', 2),
      |  (2, 'bob',    'DELETE', 3)""".stripMargin,
    // the fixture is only a fixture if it landed
    "SELECT assert_true((SELECT count(*) FROM bronze.customer_cdc) = 4, 'cdc fixture rows')",
  )

  private val source = externalTable("bronze.customer_cdc")

  /** The pipeline under test. `exceptColumnList` drops the `op` control column
    * from the target — deliberately, because the target's schema is the only
    * server-observable proof we have that the column lists crossed the wire. */
  private def cdcPipeline(target: String, once: Boolean = false): List[GraphFragment] =
    List(
      source,
      createStreamingTable(target),
      createAutoCdcFlow(
        target = target,
        source = "bronze.customer_cdc",
        keys = Seq(col("id")),
        sequenceBy = col("seq"),
        applyAsDeletes = Some(col("op") === lit("DELETE")),
        exceptColumnList = Seq(col("op")),
        once = once,
      ),
    )

  private def assemble(fragments: List[GraphFragment]) =
    ManifestAssembly
      .assemble(fragments)
      .provide(ManifestAssembly.live, GraphValidation.live)
      .mapError(errs => new RuntimeException(errs.map(_.describe).mkString("; ")))

  /** Register + run, returning either the typed registration error or the graph
    * id. Every run drains the progress stream, so the server's verdict (and its
    * event diagnostics) is what comes back. */
  private def runPipeline(
      server: SparkConnectTestServer.Server,
      fragments: List[GraphFragment],
      dry: Boolean,
  ): ZIO[Any, Throwable, Either[PipelinesRegistration.RegistrationError, String]] =
    assemble(fragments).flatMap { manifest =>
      ZIO.scoped {
        PipelinesRegistration
          .register(
            server.host,
            server.port,
            manifest,
            storage = s"file:///tmp/sdp-e2e-${UUID.randomUUID()}",
            dry = dry,
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

  /** The handshake's own probe, on its own channel — the same call
    * `PipelinesRegistration.register` makes before `CreateDataflowGraph`. */
  private def serverVersion(server: SparkConnectTestServer.Server) =
    ZIO.scoped {
      ConnectTransport.scoped(server.host, server.port, TransportConfig.plaintext).flatMap { transport =>
        PlanAnalysis.sparkVersionOn(transport, UUID.randomUUID().toString)
      }
    }

  private def rejection(e: PipelinesRegistration.RegistrationError): String = e match
    case PipelinesRegistration.RegistrationError.ServerRejected(detail) => detail
    case other                                                         => s"NOT-A-REJECTION: ${other.describe}"

  def spec =
    val tests = suite("AUTO CDC SCD1 against a live Spark 4.2.0 server (S1)")(
      test("the server this suite pins really is 4.2, and the handshake reads it") {
        // C1's handshake resolves this string and VersionGate compares it to
        // AutoCdcScd1's 4.2 minimum; registration logs `server reports Spark …`.
        for
          server  <- ZIO.service[SparkConnectTestServer.Server]
          version <- serverVersion(server)
        yield assertTrue(version.startsWith("4.2")) ?? s"server reported '$version'"
      },
      test("seeding the CDC source fixture over Connect succeeds") {
        for
          server <- ZIO.service[SparkConnectTestServer.Server]
          _      <- CatalogSeeder.run(server.host, server.port, seedStatements)
        yield assertCompletes
      },
      test("an AUTO CDC pipeline registers and passes server-side dry-run validation") {
        // The encode is exercised in full: DefineOutput for the shell, then a
        // DefineFlow whose details oneof is auto_cdc_flow_details. A 4.1 server
        // would have been refused by the handshake before this point; a
        // malformed AutoCdcFlowDetails fails inside buildAutoCdcFlow here.
        for
          server <- ZIO.service[SparkConnectTestServer.Server]
          result <- runPipeline(server, cdcPipeline("dim_customers_dry"), dry = true)
        yield assertTrue(result.exists(_.nonEmpty)) ?? s"dry run rejected: $result"
      },
      test("a real run materializes the target shell with the CDC metadata column, honoring except_column_list") {
        // The run reaches flow execution and then hits the engine's capability
        // gate (next test), but materialization has already happened — so the
        // catalog now holds the server's own answer to "what schema does an
        // AUTO CDC target get?". `op` is absent because except_column_list
        // crossed the wire; __spark_autocdc_metadata is SDP's reserved struct.
        for
          server <- ZIO.service[SparkConnectTestServer.Server]
          _      <- runPipeline(server, cdcPipeline("dim_customers"), dry = false)
          schema <- schemaOf(server, "dim_customers")
        yield assertTrue(
          schema.map(_.name) == List("id", "name", "seq", "__spark_autocdc_metadata"),
          schema.find(_.name == "__spark_autocdc_metadata").exists(_.kind == "struct"),
        ) ?? s"target schema was $schema"
      },
      test("the run stops at the target's MERGE capability — the parquet ceiling, typed") {
        // THE finding of this suite: on stock Spark 4.2.0 an AUTO CDC target
        // cannot be materialized, because nothing in the distribution
        // implements SupportsRowLevelOperations. The client is not at fault and
        // the error says so precisely; re-check when a Delta/Iceberg runtime
        // for 4.2 exists.
        for
          server <- ZIO.service[SparkConnectTestServer.Server]
          result <- runPipeline(server, cdcPipeline("dim_ceiling"), dry = false)
        yield assertTrue(
          result.left.exists(e => rejection(e).contains("AUTOCDC_TARGET_DOES_NOT_SUPPORT_MERGE")),
          result.left.exists(e => rejection(e).contains("does not support row-level operations")),
        ) ?? s"expected the MERGE-capability refusal, got: $result"
      },
      test("the server refuses a key that is not a plain column identifier") {
        // Our encoder will happily put any Ex on the wire; the server maps
        // keys/column lists through asUnqualifiedColumnName and rejects the
        // rest. Pinned here so the DSL's permissiveness stays a known quantity.
        for
          server <- ZIO.service[SparkConnectTestServer.Server]
          result <- runPipeline(
            server,
            List(
              source,
              createStreamingTable("dim_badkey"),
              createAutoCdcFlow(
                target = "dim_badkey",
                source = "bronze.customer_cdc",
                keys = Seq(col("id") + lit(1)),
                sequenceBy = col("seq"),
              ),
            ),
            dry = true,
          )
        yield assertTrue(
          result.left.exists(e => rejection(e).contains("AUTOCDC_NON_COLUMN_IDENTIFIER"))
        ) ?? s"expected AUTOCDC_NON_COLUMN_IDENTIFIER, got: $result"
      },
      test("released 4.2.0 still rejects DefineFlow.once outright (rows ONCE-1/ONCE-2)") {
        // Measured, because master and 4.1.2 both reject it and the question
        // was whether the release differs: it does not. The check is the first
        // statement of PipelinesHandler.defineFlow and tests PRESENCE, so our
        // `once` DSL surface remains unregisterable on ANY server to date —
        // which is exactly why the encoder never sends once=false.
        for
          server <- ZIO.service[SparkConnectTestServer.Server]
          result <- runPipeline(server, cdcPipeline("dim_once", once = true), dry = true)
        yield assertTrue(
          result.left.exists(e => rejection(e).contains("DEFINE_FLOW_ONCE_OPTION_NOT_SUPPORTED"))
        ) ?? s"expected the once refusal, got: $result"
      },
    ).provideShared(SparkConnectTestServer.layerFor(SparkConnectTestServer.Spark42Image))
      @@ TestAspect.withLiveEnvironment
      @@ TestAspect.sequential
      @@ TestAspect.timeout(10.minutes)

    if enabled then tests
    else tests @@ TestAspect.ignore
