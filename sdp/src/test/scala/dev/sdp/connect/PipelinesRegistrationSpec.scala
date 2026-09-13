package dev.sdp.connect

import dev.sdp.core.*
import dev.sdp.core.algebra.{Ex, Rel}
import io.grpc.{Status, StatusRuntimeException}
import org.apache.spark.connect.proto as sc
import zio.*
import zio.stream.*
import zio.test.*

import PipelinesRegistration.RegistrationError

/** The Spark Connect clients' **failure paths and sequencing**, offline.
  *
  * Before the [[ConnectTransport]] seam (P3.2) none of this could be observed
  * without a container on the other end: the registration sequence, the
  * gRPC-status → typed-verdict mapping, and the run-stream's event handling all
  * lived behind a hardcoded `ManagedChannel`. So they were effectively untested —
  * the container suites prove the HAPPY path against a real server, and the
  * interesting half is what happens when the server says no.
  *
  * The stub here is a script, not a mock: it records every `ExecutePlanRequest`
  * in order and answers from a function of (request, call index). That makes
  * "which commands did we actually send, and which did we stop sending" an
  * assertion instead of a hope.
  */
object PipelinesRegistrationSpec extends ZIOSpecDefault:

  // ---------------------------------------------------------------- fixtures

  /** Two datasets, two authored flows — enough to have a MIDDLE of the
    * `DefineOutput`/`DefineFlow` sequence to fail in. */
  private val manifest: PipelineManifest =
    PipelineManifest.fromGraphAndFlows(
      PipelineGraph(
        Map(
          "silver" -> PipelineNode.StreamingTable("silver", "delta"),
          "gold"   -> PipelineNode.StreamingTable("gold", "delta"),
        ),
        Set(DependencyEdge("silver", "gold")),
      ),
      List(
        Flow("silver_flow", "silver", FlowDetails.WriteRelation(Rel.NamedTable("bronze", streaming = true))),
        Flow("gold_flow", "gold", FlowDetails.WriteRelation(Rel.NamedTable("silver", streaming = true))),
      ),
    )

  /** An AUTO CDC pipeline: the one construct the [[VersionGate]] refuses on an
    * old server, so it exercises the "refuse before registering" branch. */
  private val cdcManifest: PipelineManifest =
    PipelineManifest.fromGraphAndFlows(
      PipelineGraph(Map("dim" -> PipelineNode.StreamingTable("dim", "delta")), Set.empty),
      List(
        Flow(
          "dim_cdc",
          "dim",
          FlowDetails.AutoCdc("bronze.cdc", List(Ex.Col("id")), Ex.Col("seq")),
        )
      ),
    )

  private def graphIdResponse(id: String): sc.ExecutePlanResponse =
    sc.ExecutePlanResponse
      .newBuilder()
      .setPipelineCommandResult(
        sc.PipelineCommandResult
          .newBuilder()
          .setCreateDataflowGraphResult(
            sc.PipelineCommandResult.CreateDataflowGraphResult.newBuilder().setDataflowGraphId(id)
          )
      )
      .build()

  private def eventResponse(message: String): sc.ExecutePlanResponse =
    sc.ExecutePlanResponse
      .newBuilder()
      .setPipelineEventResult(
        sc.PipelineEventResult
          .newBuilder()
          .setEvent(sc.PipelineEvent.newBuilder().setMessage(message))
      )
      .build()

  private def versionResponse(version: String): sc.AnalyzePlanResponse =
    sc.AnalyzePlanResponse
      .newBuilder()
      .setSparkVersion(sc.AnalyzePlanResponse.SparkVersion.newBuilder().setVersion(version))
      .build()

  private def rejected(message: String): StatusRuntimeException =
    new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription(message))

  private val unavailable: StatusRuntimeException =
    new StatusRuntimeException(Status.UNAVAILABLE.withDescription("io exception"))

  // ------------------------------------------------------------- the scripted
  //                                                                 transport

  /** The command type carried by one recorded request, as its proto enum name
    * (`CREATE_DATAFLOW_GRAPH`, `DEFINE_OUTPUT`, `DEFINE_FLOW`, `START_RUN`,
    * `DROP_DATAFLOW_GRAPH`) — the alphabet the sequencing assertions speak. */
  private def commandOf(request: sc.ExecutePlanRequest): String =
    request.getPlan.getCommand.getPipelineCommand.getCommandTypeCase.name

  private final class ScriptedTransport(
      log: Ref[Vector[sc.ExecutePlanRequest]],
      cancelled: Ref[Boolean],
      respond: (sc.ExecutePlanRequest, Int) => ZStream[Any, Throwable, sc.ExecutePlanResponse],
      analyzeWith: IO[Throwable, sc.AnalyzePlanResponse],
  ) extends ConnectTransport:
    val endpoint: String = "sc://stub:15002"

    def execute(request: sc.ExecutePlanRequest): ZStream[Any, Throwable, sc.ExecutePlanResponse] =
      ZStream.unwrap(log.modify(v => (v.size, v :+ request)).map(respond(request, _)))

    def executeUnbounded(request: sc.ExecutePlanRequest): ZStream[Any, Throwable, sc.ExecutePlanResponse] =
      execute(request)

    def analyze(request: sc.AnalyzePlanRequest): IO[Throwable, sc.AnalyzePlanResponse] = analyzeWith

    def cancel: UIO[Unit] = cancelled.set(true)

  /** One scripted server. `respond` answers by (request, 0-based call index);
    * `sent` reads back the command names in the order they went out. */
  private final case class Stub(
      transport: ConnectTransport,
      sent: UIO[List[String]],
      requests: UIO[List[sc.ExecutePlanRequest]],
      wasCancelled: UIO[Boolean],
  )

  private def stub(
      respond: (sc.ExecutePlanRequest, Int) => ZStream[Any, Throwable, sc.ExecutePlanResponse],
      analyzeWith: IO[Throwable, sc.AnalyzePlanResponse] = ZIO.succeed(versionResponse("4.2.0")),
  ): UIO[Stub] =
    for
      log       <- Ref.make(Vector.empty[sc.ExecutePlanRequest])
      cancelled <- Ref.make(false)
    yield Stub(
      new ScriptedTransport(log, cancelled, respond, analyzeWith),
      log.get.map(_.toList.map(commandOf)),
      log.get.map(_.toList),
      cancelled.get,
    )

  /** The default script: the graph id for `CreateDataflowGraph`, silence for
    * everything else (which is exactly what a happy server does — `DefineOutput`
    * / `DefineFlow` results are not read by this client). */
  private def happy(graphId: String): (sc.ExecutePlanRequest, Int) => ZStream[Any, Throwable, sc.ExecutePlanResponse] =
    (request, _) =>
      if commandOf(request) == "CREATE_DATAFLOW_GRAPH" then ZStream.succeed(graphIdResponse(graphId))
      else ZStream.empty

  private def registerOn(
      m: PipelineManifest,
      s: Stub,
      versionCheck: Boolean = true,
      dry: Boolean = true,
      fullRefresh: Boolean = false,
  ) =
    PipelinesRegistration
      .registerOn(
        manifest = m,
        storage = "file:///tmp/sdp-test",
        dry = dry,
        fullRefresh = fullRefresh,
        sqlConf = Map.empty,
        defaultCatalog = None,
        defaultDatabase = None,
        versionCheck = versionCheck,
      )
      .provideEnvironment(ZEnvironment[ConnectTransport](s.transport))

  // ------------------------------------------------------------------- specs

  def spec = suite("Spark Connect clients, off-container")(
    suite("grpcError — the gRPC status → typed verdict mapping table")(
      test("UNAVAILABLE is TRANSPORT, not a verdict: we never reached the server") {
        val mapped = PipelinesRegistration.grpcError(unavailable, Nil)
        assertTrue(
          mapped.isInstanceOf[RegistrationError.TransportFailure],
          mapped.describe.contains("server unreachable"),
          mapped.describe.contains("io exception"),
        )
      },
      test("any other gRPC status is the SERVER's verdict, carrying code + description") {
        val mapped = PipelinesRegistration.grpcError(rejected("TABLE_OR_VIEW_NOT_FOUND: bronze"), Nil)
        assertTrue(
          mapped.isInstanceOf[RegistrationError.ServerRejected],
          mapped.describe.contains("INVALID_ARGUMENT"),
          mapped.describe.contains("TABLE_OR_VIEW_NOT_FOUND: bronze"),
        )
      },
      test("a non-gRPC throwable is transport (a broken socket, a codec blow-up)") {
        val mapped = PipelinesRegistration.grpcError(new java.io.IOException("connection reset"), Nil)
        assertTrue(
          mapped.isInstanceOf[RegistrationError.TransportFailure],
          mapped.describe.contains("connection reset"),
        )
      },
      test("events seen BEFORE the failure are attached — they are the real diagnostic") {
        val mapped = PipelinesRegistration.grpcError(
          rejected("run failed"),
          List("Flow 'silver_flow' is QUEUED.", "Flow 'silver_flow' has FAILED."),
        )
        assertTrue(
          mapped.describe.contains("server events:"),
          mapped.describe.contains("Flow 'silver_flow' is QUEUED."),
          mapped.describe.contains("Flow 'silver_flow' has FAILED."),
        )
      },
      test("no events means no empty 'server events' preamble") {
        assertTrue(!PipelinesRegistration.grpcError(rejected("nope"), Nil).describe.contains("server events"))
      },
    ),
    suite("registration sequencing")(
      test("the happy path sends create → outputs → flows, and nothing else") {
        for
          s      <- stub(happy("graph-1"))
          handle <- registerOn(manifest, s)
          sent   <- s.sent
        yield assertTrue(
          handle.graphId == "graph-1",
          sent.head == "CREATE_DATAFLOW_GRAPH",
          sent.count(_ == "DEFINE_OUTPUT") == 2,
          sent.count(_ == "DEFINE_FLOW") == 2,
          // StartRun is NOT sent by register: it is the RunHandle's stream, and
          // nothing has drained it yet.
          !sent.contains("START_RUN"),
          !sent.contains("DROP_DATAFLOW_GRAPH"),
        )
      },
      test("a server that answers CreateDataflowGraph with no id is a rejection, not a defect") {
        for
          s    <- stub((_, _) => ZStream.empty)
          exit <- registerOn(manifest, s).exit
          sent <- s.sent
        yield assertTrue(
          exit.isFailure,
          sent == List("CREATE_DATAFLOW_GRAPH"),
        ) && assert(exit)(
          Assertion.failsWithA[RegistrationError.ServerRejected]
        )
      },
      test("a mid-sequence DefineFlow rejection stops the sequence THERE") {
        // Reject the 4th command (index 3): create, output, output, FLOW← .
        for
          s <- stub((request, index) =>
                 if index == 3 then ZStream.fail(rejected("UNRESOLVED_COLUMN: amount"))
                 else happy("graph-9")(request, index)
               )
          exit <- registerOn(manifest, s).exit
          sent <- s.sent
        yield assertTrue(
          exit.isFailure,
          // everything up to and including the rejected command went out …
          sent.take(4) == List("CREATE_DATAFLOW_GRAPH", "DEFINE_OUTPUT", "DEFINE_OUTPUT", "DEFINE_FLOW"),
          // … and the SECOND DefineFlow never did.
          sent.count(_ == "DEFINE_FLOW") == 1,
        ) && assert(exit)(Assertion.failsWithA[RegistrationError.ServerRejected])
      },
      test("…and drops the half-built graph it left on the server") {
        for
          s <- stub((request, index) =>
                 if index == 3 then ZStream.fail(rejected("UNRESOLVED_COLUMN: amount"))
                 else happy("graph-9")(request, index)
               )
          _    <- registerOn(manifest, s).exit
          sent <- s.sent
          reqs <- s.requests
          drop = reqs.find(r => commandOf(r) == "DROP_DATAFLOW_GRAPH")
        yield assertTrue(
          sent.last == "DROP_DATAFLOW_GRAPH",
          drop.exists(
            _.getPlan.getCommand.getPipelineCommand.getDropDataflowGraph.getDataflowGraphId == "graph-9"
          ),
        )
      },
      test("a failing drop is swallowed — the ORIGINAL verdict is what the author must read") {
        for
          s <- stub((request, index) =>
                 if commandOf(request) == "DROP_DATAFLOW_GRAPH" then ZStream.fail(rejected("drop exploded"))
                 else if index == 3 then ZStream.fail(rejected("UNRESOLVED_COLUMN: amount"))
                 else happy("graph-9")(request, index)
               )
          exit <- registerOn(manifest, s).exit
        yield assert(exit)(Assertion.fails(Assertion.hasField[RegistrationError, String](
          "describe",
          _.describe,
          Assertion.containsString("UNRESOLVED_COLUMN: amount"),
        )))
      },
      test("the events a rejected command emitted first are folded into the error") {
        for
          s <- stub((request, index) =>
                 if index == 3 then
                   ZStream(eventResponse("Failed to resolve flow 'gold_flow'.")) ++
                     ZStream.fail(rejected("ANALYSIS_ERROR"))
                 else happy("graph-9")(request, index)
               )
          exit <- registerOn(manifest, s).exit
        yield assert(exit)(Assertion.fails(Assertion.hasField[RegistrationError, String](
          "describe",
          _.describe,
          Assertion.containsString("Failed to resolve flow 'gold_flow'."),
        )))
      },
    ),
    suite("the version handshake, before anything is registered")(
      test("an AUTO CDC pipeline against a 4.1 server refuses with NOTHING sent") {
        for
          s    <- stub(happy("graph-1"), analyzeWith = ZIO.succeed(versionResponse("4.1.0")))
          exit <- registerOn(cdcManifest, s).exit
          sent <- s.sent
        yield assertTrue(sent.isEmpty) && assert(exit)(
          Assertion.failsWithA[RegistrationError.ServerTooOld]
        )
      },
      test("a version the client cannot read is a warning, not a refusal") {
        for
          s      <- stub(happy("graph-1"), analyzeWith = ZIO.succeed(versionResponse("vendor-edition")))
          handle <- registerOn(cdcManifest, s)
        yield assertTrue(handle.graphId == "graph-1")
      },
      test("a failed probe proceeds — the next RPC reports the transport problem better") {
        for
          s      <- stub(happy("graph-1"), analyzeWith = ZIO.fail(unavailable))
          handle <- registerOn(manifest, s)
          sent   <- s.sent
        yield assertTrue(handle.graphId == "graph-1", sent.head == "CREATE_DATAFLOW_GRAPH")
      },
      test("versionCheck = false never probes at all") {
        for
          s <- stub(
                 happy("graph-1"),
                 analyzeWith = ZIO.die(new AssertionError("the handshake must not run")),
               )
          handle <- registerOn(manifest, s, versionCheck = false)
        yield assertTrue(handle.graphId == "graph-1")
      },
    ),
    suite("the run stream")(
      test("StartRun is sent only when the progress stream is drained, and events parse") {
        val messages = List(
          "Flow 'silver_flow' is QUEUED.",
          "Flow 'silver_flow' is RUNNING.",
          "Flow 'silver_flow' has COMPLETED.",
        )
        for
          s <- stub((request, index) =>
                 if commandOf(request) == "START_RUN" then ZStream.fromIterable(messages.map(eventResponse))
                 else happy("graph-1")(request, index)
               )
          handle  <- registerOn(manifest, s)
          before  <- s.sent
          events  <- handle.progress.runCollect
          after   <- s.sent
        yield assertTrue(
          !before.contains("START_RUN"),
          after.count(_ == "START_RUN") == 1,
          events.map(_.state).toList == List(FlowState.Queued, FlowState.Running, FlowState.Completed),
          events.map(_.flow).toList.forall(_ == Some("silver_flow")),
        )
      },
      test("a full refresh reaches the wire as StartRun.full_refresh_all, and only then") {
        // The plumb, end to end through the registration sequence: what the
        // caller asked for is what the SERVER is told. Both runs are driven
        // through the same stub so the comparison is of bytes, not of intent —
        // an ordinary run must leave the field off the wire entirely (proto3
        // presence, so "absent" and "present false" are different messages).
        def startRun(requests: List[sc.ExecutePlanRequest]) =
          requests
            .map(_.getPlan.getCommand.getPipelineCommand)
            .find(_.hasStartRun)
            .map(_.getStartRun)
        val field = sc.PipelineCommand.StartRun.getDescriptor.findFieldByName("full_refresh_all")
        for
          plainStub <- stub(happy("graph-1"))
          plain     <- registerOn(manifest, plainStub, dry = false)
          _         <- plain.progress.runDrain
          plainReqs <- plainStub.requests
          fullStub  <- stub(happy("graph-1"))
          full      <- registerOn(manifest, fullStub, dry = false, fullRefresh = true)
          _         <- full.progress.runDrain
          fullReqs  <- fullStub.requests
        yield assertTrue(
          startRun(plainReqs).exists(!_.hasField(field)),
          startRun(fullReqs).exists(_.getFullRefreshAll),
          // the rest of the command is untouched — same graph, same storage
          startRun(fullReqs).map(_.getStorage) == startRun(plainReqs).map(_.getStorage),
          startRun(fullReqs).map(_.getDataflowGraphId) == startRun(plainReqs).map(_.getDataflowGraphId),
          // and a full refresh changes nothing about the registration that precedes it
          plainReqs.map(commandOf) == fullReqs.map(commandOf),
        )
      },
      test("responses that are not events, and events with an empty message, are skipped") {
        for
          s <- stub((request, index) =>
                 if commandOf(request) == "START_RUN" then
                   ZStream(
                     sc.ExecutePlanResponse.newBuilder().build(), // no event at all
                     eventResponse(""),                           // event, empty message
                     eventResponse("Run is COMPLETED."),
                   )
                 else happy("graph-1")(request, index)
               )
          handle <- registerOn(manifest, s)
          events <- handle.progress.runCollect
        yield assertTrue(
          events.size == 1,
          // a run-level event has no flow attribution, by design
          events.head.flow.isEmpty,
          events.head.state == FlowState.Completed,
        )
      },
      test("EV-7: a FAILED event's multi-line message survives the stream intact") {
        // The shape Spark 4.2.0 actually emits (measured, BehaviorInventory
        // EV-7): the verdict line, then a blank-prefixed Error block carrying
        // the diagnostic. The TRANSPORT must not truncate or split it.
        val failure =
          "Flow 'silver_flow' has FAILED.\nError: [TABLE_OR_VIEW_NOT_FOUND] The table or view `bronze` cannot be found."
        for
          s <- stub((request, index) =>
                 if commandOf(request) == "START_RUN" then ZStream.succeed(eventResponse(failure))
                 else happy("graph-1")(request, index)
               )
          handle <- registerOn(manifest, s)
          events <- handle.progress.runCollect
        yield assertTrue(
          events.size == 1,
          events.head.raw == failure,
          events.head.raw.contains("TABLE_OR_VIEW_NOT_FOUND"),
          events.head.state == FlowState.Failed,
          // …and the KNOWN gap, asserted so it cannot regress silently: the
          // flow attribution is lost, because `RunProgress.flowName`'s regexes
          // are single-line (`.*` does not cross \n). That parser is pure
          // Domain Core and its fix is scheduled with its cousin EV-3 in P3.4 —
          // the transport seam is not where it belongs.
          events.head.flow.isEmpty,
        )
      },
      test("a mid-run gRPC failure surfaces as the stream's typed error") {
        for
          s <- stub((request, index) =>
                 if commandOf(request) == "START_RUN" then
                   ZStream(eventResponse("Flow 'silver_flow' is RUNNING.")) ++ ZStream.fail(rejected("boom"))
                 else happy("graph-1")(request, index)
               )
          handle <- registerOn(manifest, s)
          exit   <- handle.progress.runCollect.exit
        yield assert(exit)(Assertion.failsWithA[RegistrationError.ServerRejected])
      },
      test("RunHandle.cancel force-closes the transport") {
        for
          s         <- stub(happy("graph-1"))
          handle    <- registerOn(manifest, s)
          before    <- s.wasCancelled
          _         <- handle.cancel
          after     <- s.wasCancelled
        yield assertTrue(!before, after)
      },
    ),
    suite("CatalogSeeder over the seam")(
      test("statements go out in order, on ONE session, as SQL plans") {
        val statements = List("CREATE SCHEMA bronze", "INSERT INTO bronze.t VALUES (1)")
        for
          s    <- stub((_, _) => ZStream.empty)
          _    <- CatalogSeeder.runOn(statements).provideEnvironment(ZEnvironment[ConnectTransport](s.transport))
          reqs <- s.requests
        yield assertTrue(
          reqs.map(_.getPlan.getRoot.getSql.getQuery) == statements,
          reqs.map(_.getSessionId).distinct.size == 1,
        )
      },
      test("the first rejection stops the list and names the offending statement") {
        for
          s <- stub((_, index) => if index == 1 then ZStream.fail(rejected("PARSE_SYNTAX_ERROR")) else ZStream.empty)
          exit <- CatalogSeeder
            .runOn(List("SELECT 1", "SELCT 2", "SELECT 3"))
            .provideEnvironment(ZEnvironment[ConnectTransport](s.transport))
            .exit
          reqs <- s.requests
        yield assertTrue(reqs.size == 2) && assert(exit)(
          Assertion.fails(
            Assertion.hasField[RegistrationError, String](
              "describe",
              _.describe,
              Assertion.containsString("PARSE_SYNTAX_ERROR") && Assertion.containsString("in: SELCT 2"),
            )
          )
        )
      },
      test("a transport failure keeps the statement context too") {
        for
          s <- stub((_, _) => ZStream.fail(new java.io.IOException("socket closed")))
          exit <- CatalogSeeder
            .runOn(List("SELECT 1"))
            .provideEnvironment(ZEnvironment[ConnectTransport](s.transport))
            .exit
        yield assert(exit)(
          Assertion.fails(
            Assertion.hasField[RegistrationError, String](
              "describe",
              _.describe,
              Assertion.containsString("socket closed") && Assertion.containsString("in: SELECT 1"),
            )
          )
        )
      },
    ),
    suite("PlanAnalysis over the seam")(
      test("the analyzed struct becomes name/kind pairs") {
        val schema = sc.AnalyzePlanResponse
          .newBuilder()
          .setSchema(
            sc.AnalyzePlanResponse.Schema
              .newBuilder()
              .setSchema(
                sc.DataType
                  .newBuilder()
                  .setStruct(
                    sc.DataType.Struct
                      .newBuilder()
                      .addFields(
                        sc.DataType.StructField
                          .newBuilder()
                          .setName("id")
                          .setDataType(sc.DataType.newBuilder().setLong(sc.DataType.Long.newBuilder()))
                      )
                      .addFields(
                        sc.DataType.StructField
                          .newBuilder()
                          .setName("name")
                          .setDataType(sc.DataType.newBuilder().setString(sc.DataType.String.newBuilder()))
                      )
                  )
              )
          )
          .build()
        for
          s <- stub((_, _) => ZStream.empty, analyzeWith = ZIO.succeed(schema))
          fields <- PlanAnalysis
            .analyzeSchemaOn(AlgebraProtoEncoder.relation(Rel.NamedTable("bronze", streaming = false)))
            .provideEnvironment(ZEnvironment[ConnectTransport](s.transport))
        yield assertTrue(
          fields == List(PlanAnalysis.SchemaField("id", "long"), PlanAnalysis.SchemaField("name", "string"))
        )
      },
      test("an analyzer rejection is the server's verdict; an IO failure is transport") {
        for
          s1 <- stub((_, _) => ZStream.empty, analyzeWith = ZIO.fail(rejected("UNRESOLVED_RELATION")))
          e1 <- PlanAnalysis
            .analyzeSchemaOn(AlgebraProtoEncoder.relation(Rel.NamedTable("nope", streaming = false)))
            .provideEnvironment(ZEnvironment[ConnectTransport](s1.transport))
            .exit
          s2 <- stub((_, _) => ZStream.empty, analyzeWith = ZIO.fail(new java.io.IOException("reset")))
          e2 <- PlanAnalysis
            .analyzeSchemaOn(AlgebraProtoEncoder.relation(Rel.NamedTable("nope", streaming = false)))
            .provideEnvironment(ZEnvironment[ConnectTransport](s2.transport))
            .exit
        yield assert(e1)(Assertion.failsWithA[RegistrationError.ServerRejected]) &&
          assert(e2)(Assertion.failsWithA[RegistrationError.TransportFailure])
      },
    ),
  )
