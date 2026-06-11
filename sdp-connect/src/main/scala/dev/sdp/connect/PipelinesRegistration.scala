package dev.sdp.connect

import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters.*

import dev.sdp.core.{PipelineManifest, RunProgress}
import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}
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

    def describe: String = this match
      case TransportFailure(d) => s"Spark Connect transport failure: $d"
      case ServerRejected(d)   => s"Spark Connect server rejected the pipeline: $d"

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
  ): ZIO[Scope, RegistrationError, RunHandle] =
    channel(host, port).flatMap { ch =>
      val stub      = sc.SparkConnectServiceGrpc.newBlockingStub(ch)
      val sessionId = UUID.randomUUID().toString

      for
        created <- execute(stub, sessionId, PipelineProtoEncoder.createDataflowGraph(sqlConf = sqlConf))
        graphId <- ZIO
          .fromOption(created.collectFirst {
            case r if r.hasPipelineCommandResult && r.getPipelineCommandResult.hasCreateDataflowGraphResult =>
              r.getPipelineCommandResult.getCreateDataflowGraphResult.getDataflowGraphId
          })
          .orElseFail(RegistrationError.ServerRejected("CreateDataflowGraph returned no graph id"))
        _ <- ZIO.foreachDiscard(PipelineProtoEncoder.definitions(graphId, manifest)) { cmd =>
          execute(stub, sessionId, cmd)
        }
      yield RunHandle(
        graphId,
        runStream(stub, sessionId, PipelineProtoEncoder.startRun(graphId, dry, storage)),
        cancel = ZIO.succeed { ch.shutdownNow(); () },
      )
    }

  /** The `StartRun` server-stream as a `ZStream` of parsed progress events.
    *
    * `ZStream.fromBlockingIterator` runs the blocking gRPC iterator on the
    * blocking pool and owns its interruption/cleanup — so `.runDrain.timeout`
    * cancels a never-terminating (continuous) run cleanly, no manual
    * channel-cancel needed. Failures (the gRPC status) surface as the stream's
    * error; the progress events leading up to a failure have already been
    * emitted (and logged), so they are the error context. */
  private def runStream(
      stub: sc.SparkConnectServiceGrpc.SparkConnectServiceBlockingStub,
      sessionId: String,
      command: sc.PipelineCommand,
  ): ZStream[Any, RegistrationError, RunProgress] =
    ZStream
      .blocking(ZStream.fromJavaIterator(stub.executePlan(executeRequest(sessionId, command))))
      .collect {
        case r if r.hasPipelineEventResult && r.getPipelineEventResult.getEvent.getMessage.nonEmpty =>
          RunProgress.parse(r.getPipelineEventResult.getEvent.getMessage)
      }
      .mapError(grpcError(_, Nil))

  /** A scoped gRPC channel: acquisition and guaranteed shutdown in one place
    * (Zionomicon ch. 14/15 — the resource cannot leak past the scope).
    */
  private def channel(host: String, port: Int): ZIO[Scope, RegistrationError, ManagedChannel] =
    ZIO
      .acquireRelease(
        ZIO.attemptBlocking(
          ManagedChannelBuilder.forAddress(host, port).usePlaintext().build()
        )
      )(ch =>
        ZIO.attemptBlocking {
          ch.shutdownNow()
          val _ = ch.awaitTermination(10, TimeUnit.SECONDS)
        }.orDie
      )
      .mapError(e => RegistrationError.TransportFailure(e.toString))

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
      command: sc.PipelineCommand,
  ): IO[RegistrationError, List[sc.ExecutePlanResponse]] =
    ZIO.suspendSucceed {
      val request  = executeRequest(sessionId, command)
      val received = scala.collection.mutable.ListBuffer.empty[sc.ExecutePlanResponse]
      ZIO
        .attemptBlocking {
          val it = stub.executePlan(request)
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
