package dev.sdp.connect

import java.util.UUID

import dev.sdp.core.{PipelineManifest, RunProgress}
import io.grpc.StatusRuntimeException
import org.apache.spark.connect.proto as sc
import zio.*
import zio.stream.*

/** The Spark Connect registration sequence, as a typed ZIO effect.
  *
  * Wire mechanics (all verified against `../spark` source):
  *   - `PipelineCommand`s ride inside `ExecutePlanRequest.plan.command
  *     .pipeline_command` (`commands.proto`, field 19);
  *   - results come back on the response stream as
  *     `ExecutePlanResponse.pipeline_command_result` (`base.proto`, field 22);
  *   - the sequence is: `CreateDataflowGraph` (server returns the graph id)
  *     → every `DefineOutput`/`DefineFlow` → `StartRun(dry = true)` for
  *     server-side validation without execution.
  *
  * Error model (Zionomicon ch. 3): transport and server rejections are
  * *expected* failures a build must render readably — they live in the typed
  * channel as [[RegistrationError]]; anything else is a defect.
  */
object PipelinesRegistration:

  enum RegistrationError:
    case TransportFailure(detail: String)
    case ServerRejected(detail: String)

    /** The manifest carries a construct the pinned wire client cannot encode
      * (see [[UnsupportedWireFeature]] — AUTO CDC before the 4.2 proto). An
      * expected, renderable verdict: `validate`/`manifest` accepted the graph
      * offline, so the author meets it here and must read a sentence, not a
      * defect trace. */
    case UnsupportedWire(detail: String)

    /** The server is older than a construct the pipeline uses — caught by the
      * [[VersionGate]] handshake BEFORE anything is registered, because proto3
      * drops unknown fields and an old server would otherwise fail
      * confusingly (or silently misbehave). Expected and renderable: the author
      * reads which flow, which version, which endpoint. */
    case ServerTooOld(detail: String)

    def describe: String = this match
      case TransportFailure(d) => s"Spark Connect transport failure: $d"
      case ServerRejected(d)   => s"Spark Connect server rejected the pipeline: $d"
      case UnsupportedWire(d)  => s"unsupported by the pinned Spark Connect wire client: $d"
      case ServerTooOld(d)     => s"Spark Connect server is too old for this pipeline: $d"

  /** A registered graph plus its run as a **stream** of progress events.
    *
    * `graphId` is known eagerly (from `CreateDataflowGraph`); `progress` is the
    * `StartRun` server-stream, modelled as a `ZStream` (Zionomicon ch.36:
    * server streaming = `ZStream`). Consumers compose it — `.tap` to log,
    * `.broadcast` to also drive a live DAG render, `.runDrain.timeout` to bound
    * the run. `ZStream.fromBlockingIterator` owns the blocking gRPC iterator's
    * interruption + cleanup, so a timed-out run cancels cleanly (no manual
    * `attemptBlockingCancelable`/`.disconnect` dance). */
  final case class RunHandle(
      graphId: String,
      progress: ZStream[Any, RegistrationError, RunProgress],
      /** Force-close the channel — unblocks a `progress` pull parked in a
        * never-terminating run so a timeout can detach. Race the drain against
        * `ZIO.sleep(t) *> cancel` rather than relying on `.timeout` alone (a
        * parked blocking `next()` won't observe interruption until the channel
        * closes — ch08: interrupt waits for finalizers). */
      cancel: UIO[Unit],
  )

  /** Register the manifest and start a run. Succeeds with the
    * server-assigned dataflow graph id.
    *
    * @param storage pipeline checkpoint/metadata root — the server demands
    *                an absolute URI with a scheme even for dry runs
    * @param dry     when true (the safe default) the server only validates;
    *                no flows execute
    */
  def register(
      host: String,
      port: Int,
      manifest: PipelineManifest,
      storage: String = "file:///tmp/sdp-dry-run",
      dry: Boolean = true,
      sqlConf: Map[String, String] = Map.empty,
      // Graph defaults (CreateDataflowGraph fields 1/2). The official Python
      // client ALWAYS sends them; omitting routes the server onto a session
      // fallback that mis-qualifies reads on named V2 catalogs — dependency
      // edges silently vanish and dependent flows race. None = omit (legacy).
      defaultCatalog: Option[String] = None,
      defaultDatabase: Option[String] = None,
      // Transport security + per-RPC deadline. Defaults to plaintext/anonymous:
      // sc://localhost is the dev container and must keep working untouched.
      transport: TransportConfig = TransportConfig.plaintext,
      // The server-version handshake (VersionGate). ON by default; the escape
      // hatch (SDP_SKIP_VERSION_CHECK / sdpVersionCheck := false) exists for a
      // fork that reports a version we would read wrongly.
      versionCheck: Boolean = true,
  ): ZIO[Scope, RegistrationError, RunHandle] =
    ConnectChannel.scoped(host, port, transport).flatMap { ch =>
      val stub      = sc.SparkConnectServiceGrpc.newBlockingStub(ch)
      val sessionId = UUID.randomUUID().toString

      for
        // Handshake FIRST: ask what the server is, and refuse before a single
        // dataset is registered if the pipeline needs a newer wire than it
        // speaks. Nothing is created server-side when this fails.
        _ <- handshake(stub, sessionId, transport, manifest, s"sc://$host:$port", versionCheck)
        created <- execute(
                     stub,
                     sessionId,
                     transport,
                     PipelineProtoEncoder.createDataflowGraph(
                       defaultCatalog = defaultCatalog,
                       defaultDatabase = defaultDatabase,
                       sqlConf = sqlConf,
                     ),
                   )
        graphId <- ZIO
          .fromOption(created.collectFirst {
            case r if r.hasPipelineCommandResult && r.getPipelineCommandResult.hasCreateDataflowGraphResult =>
              r.getPipelineCommandResult.getCreateDataflowGraphResult.getDataflowGraphId
          })
          .orElseFail(RegistrationError.ServerRejected("CreateDataflowGraph returned no graph id"))
        // Encoding is pure but gated: an unencodable construct throws
        // UnsupportedWireFeature. Refine it into the typed channel so the
        // runner renders a sentence instead of a defect trace.
        commands <- ZIO
          .attempt(PipelineProtoEncoder.definitions(graphId, manifest))
          .refineOrDie { case e: UnsupportedWireFeature =>
            RegistrationError.UnsupportedWire(e.getMessage)
          }
        _ <- ZIO.foreachDiscard(commands)(execute(stub, sessionId, transport, _))
      yield RunHandle(
        graphId,
        runStream(stub, sessionId, PipelineProtoEncoder.startRun(graphId, dry, storage)),
        cancel = ZIO.succeed { ch.shutdownNow(); () },
      )
    }

  /** The server-version handshake: one `AnalyzePlan`/`SparkVersion` round trip,
    * then the pure [[VersionGate]].
    *
    * Three outcomes, all deliberate:
    *   - **recognised and new enough** — log `server reports Spark X.Y.Z` at
    *     info and continue (every registration says which server it talked to;
    *     half the confusing bug reports in this project's history were "which
    *     server was that?");
    *   - **recognised and too old** — fail in the typed channel, before
    *     `CreateDataflowGraph`, so nothing is half-registered;
    *   - **unrecognised, or the probe itself failed** — warn and PROCEED. A
    *     fork reporting an odd string must not be blocked by us, and a real
    *     transport problem is about to be reported much better by the next RPC.
    *
    * `versionCheck = false` skips the round trip entirely and says so.
    */
  private def handshake(
      stub: sc.SparkConnectServiceGrpc.SparkConnectServiceBlockingStub,
      sessionId: String,
      transport: TransportConfig,
      manifest: PipelineManifest,
      endpoint: String,
      versionCheck: Boolean,
  ): IO[RegistrationError, Unit] =
    if !versionCheck then
      ZIO.logWarning(
        s"server-version check DISABLED for $endpoint — a construct newer than the server " +
          "will fail confusingly or silently misbehave (proto3 drops unknown fields)"
      )
    else
      PlanAnalysis.sparkVersionOn(stub, sessionId, transport).either.flatMap {
        case Left(err) =>
          ZIO.logWarning(
            s"could not resolve the Spark version of $endpoint (${err.describe}) — " +
              "proceeding without the version check"
          )
        case Right(reported) =>
          val recognised = VersionGate.ServerVersion.parse(reported)
          val announce =
            if recognised.isDefined then ZIO.logInfo(s"server reports Spark $reported")
            else
              ZIO.logWarning(
                s"server reports Spark '$reported', which this client cannot read as a version — " +
                  "proceeding without the version check"
              )
          announce *> ZIO.fromEither(VersionGate.check(reported, manifest, endpoint))
      }

  /** The `StartRun` server-stream as a `ZStream` of parsed progress events.
    *
    * Each pull is an `attemptBlockingInterrupt`, so ZIO interrupts the blocking
    * thread itself when the drain loses a race — a parked `next()` does not keep
    * a finalizer waiting. The channel-level [[RunHandle.cancel]] stays as the
    * definitive escape hatch for a server that never writes anything at all.
    * NO deadline here on purpose: a real run legitimately takes minutes, and the
    * caller bounds it by racing the drain against `cancel`.
    *
    * Failures (the gRPC status) surface as the stream's error; the progress
    * events leading up to a failure have already been emitted (and logged), so
    * they are the error context. */
  private def runStream(
      stub: sc.SparkConnectServiceGrpc.SparkConnectServiceBlockingStub,
      sessionId: String,
      command: sc.PipelineCommand,
  ): ZStream[Any, RegistrationError, RunProgress] =
    ZStream
      .fromZIO(ZIO.attemptBlockingInterrupt(stub.executePlan(executeRequest(sessionId, command))))
      .flatMap { responses =>
        ZStream.unfoldZIO(responses) { it =>
          ZIO.attemptBlockingInterrupt(if it.hasNext then Some((it.next(), it)) else None)
        }
      }
      .collect {
        case r if r.hasPipelineEventResult && r.getPipelineEventResult.getEvent.getMessage.nonEmpty =>
          RunProgress.parse(r.getPipelineEventResult.getEvent.getMessage)
      }
      .mapError(grpcError(_, Nil))

  /** Run one command through the blocking stub, draining the response
    * stream. gRPC status errors map to the typed channel.
    *
    * Pipeline events received *before* a failure are attached to the error:
    * the analyzer's real diagnostics arrive as `PipelineEventResult` stream
    * messages, and an error without them just says "something failed".
    */
  private def execute(
      stub: sc.SparkConnectServiceGrpc.SparkConnectServiceBlockingStub,
      sessionId: String,
      transport: TransportConfig,
      command: sc.PipelineCommand,
  ): IO[RegistrationError, List[sc.ExecutePlanResponse]] =
    ZIO.suspendSucceed {
      val request  = executeRequest(sessionId, command)
      val received = scala.collection.mutable.ListBuffer.empty[sc.ExecutePlanResponse]
      ZIO
        // Registration commands only write metadata: a deadline keeps a wedged
        // or unreachable-but-accepting server from hanging the build forever,
        // and attemptBlockingInterrupt lets an interrupt actually land.
        .attemptBlockingInterrupt {
          val it = ConnectChannel.withDeadline(stub, transport).executePlan(request)
          while it.hasNext do received += it.next()
          received.toList
        }
        .mapError(grpcError(_, eventStrings(received.toList)))
    }

  private def executeRequest(sessionId: String, command: sc.PipelineCommand): sc.ExecutePlanRequest =
    sc.ExecutePlanRequest
      .newBuilder()
      .setSessionId(sessionId)
      .setUserContext(sc.UserContext.newBuilder().setUserId("sbt-spark-pipelines"))
      .setPlan(sc.Plan.newBuilder().setCommand(sc.Command.newBuilder().setPipelineCommand(command)))
      .build()

  private def eventStrings(responses: List[sc.ExecutePlanResponse]): List[String] =
    responses
      .filter(_.hasPipelineEventResult)
      .map(_.getPipelineEventResult.getEvent.getMessage)
      .filter(_.nonEmpty)

  /** Map a gRPC failure to the typed channel, attaching server events seen
    * before it (the analyzer's real diagnostics arrive as stream messages). */
  private def grpcError(cause: Throwable, events: List[String]): RegistrationError =
    val context = if events.isEmpty then "" else events.mkString("\nserver events:\n  - ", "\n  - ", "")
    cause match
            case e: StatusRuntimeException if e.getStatus.getCode == io.grpc.Status.Code.UNAVAILABLE =>
              // Couldn't reach the server at all — that's transport, not a verdict.
              RegistrationError.TransportFailure(s"server unreachable: ${e.getStatus.getDescription}")
            case e: StatusRuntimeException =>
              RegistrationError.ServerRejected(s"${e.getStatus.getCode}: ${e.getStatus.getDescription}$context")
            case other =>
              RegistrationError.TransportFailure(s"$other$context")
