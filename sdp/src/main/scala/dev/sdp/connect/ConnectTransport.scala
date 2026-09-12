package dev.sdp.connect

import java.util.concurrent.TimeUnit

import io.grpc.{ManagedChannel, ManagedChannelBuilder, Metadata}
import io.grpc.stub.{AbstractStub, MetadataUtils}
import org.apache.spark.connect.proto as sc
import zio.*
import zio.stream.*

import PipelinesRegistration.RegistrationError

/** How to reach a Spark Connect server: transport security, credentials, and
  * the per-RPC deadline.
  *
  * Every channel in this package used to hardcode `usePlaintext()` with no way
  * to authenticate, so the library only worked against an unauthenticated
  * server on a trusted network. TLS + a bearer token is the shape every managed
  * Connect endpoint needs (the recipe is the one proven in `DatabricksProbe`:
  * no `usePlaintext()` → TLS, plus an `Authorization: Bearer …` header attached
  * to every call).
  *
  * Plaintext stays the DEFAULT: `sc://localhost:15002` is the dev container, and
  * silently upgrading it to TLS would break the inner loop.
  *
  * The token is a secret. [[toString]] redacts it so it cannot reach a log line
  * through config echoing, `ZIO.debug`, or an error message that interpolates
  * the config.
  */
final case class TransportConfig(
    useTls: Boolean = false,
    token: Option[String] = None,
    deadlineSeconds: Long = TransportConfig.DefaultDeadlineSeconds,
):
  /** Never print the token. */
  override def toString: String =
    s"TransportConfig(useTls=$useTls, token=${if token.isDefined then "<redacted>" else "none"}, " +
      s"deadlineSeconds=$deadlineSeconds)"

object TransportConfig:

  /** Unary RPCs (CreateDataflowGraph, DefineOutput/Flow, AnalyzePlan, seed SQL)
    * must not hang forever on a wedged server — 60s is generous for a call that
    * only registers metadata. The `StartRun` server-stream is deliberately NOT
    * deadlined: a run legitimately takes minutes and is bounded by the caller's
    * drain-vs-cancel race instead. */
  val DefaultDeadlineSeconds: Long = 60L

  /** The dev-container default: plaintext, anonymous. */
  val plaintext: TransportConfig = TransportConfig()

  /** The transport decision as DATA — `Plaintext` vs `Tls`, and whether a
    * bearer credential is attached. There is no live TLS server to test
    * against, so the DECISION is the unit under test, not the socket. */
  enum Security:
    case Plaintext
    case Tls

  final case class ChannelPlan(
      host: String,
      port: Int,
      security: Security,
      bearer: Boolean,
      deadlineSeconds: Long,
  )

  /** Pure: what a channel builder must do for this target. */
  def plan(host: String, port: Int, config: TransportConfig): ChannelPlan =
    ChannelPlan(
      host = host,
      port = port,
      security = if config.useTls then Security.Tls else Security.Plaintext,
      bearer = config.token.isDefined,
      deadlineSeconds = config.deadlineSeconds,
    )

  /** Parse raw configuration strings (env vars or sbt settings). Total: a
    * malformed value is a `Left` naming the offending setting and value, so the
    * caller can render one readable line instead of dying.
    *
    * @param useTls  unset/empty = plaintext (the localhost default)
    * @param token   unset/empty/blank = anonymous; trimmed otherwise
    */
  def parse(
      useTls: Option[String],
      token: Option[String],
      deadlineSeconds: Option[String] = None,
      tlsVarName: String = "SDP_CONNECT_USE_TLS",
      deadlineVarName: String = "SDP_CONNECT_DEADLINE",
  ): Either[String, TransportConfig] =
    for
      tls <- parseBoolean(tlsVarName, useTls)
      dl  <- parsePositiveLong(deadlineVarName, deadlineSeconds)
    yield TransportConfig(
      useTls = tls,
      token = token.map(_.trim).filter(_.nonEmpty),
      deadlineSeconds = dl.getOrElse(DefaultDeadlineSeconds),
    )

  /** `true/1/yes/on` and `false/0/no/off` (any case); unset/empty = false. */
  private def parseBoolean(name: String, raw: Option[String]): Either[String, Boolean] =
    raw.map(_.trim.toLowerCase).filter(_.nonEmpty) match
      case None                                          => Right(false)
      case Some("true" | "1" | "yes" | "on")             => Right(true)
      case Some("false" | "0" | "no" | "off")            => Right(false)
      case Some(other) =>
        Left(s"$name must be true or false, got '$other'")

  private def parsePositiveLong(name: String, raw: Option[String]): Either[String, Option[Long]] =
    raw.map(_.trim).filter(_.nonEmpty) match
      case None => Right(None)
      case Some(value) =>
        value.toLongOption.filter(_ > 0) match
          case Some(n) => Right(Some(n))
          case None    => Left(s"$name must be a positive number of seconds, got '$value'")

/** The one place a Spark Connect channel is built, for every caller in this
  * package (registration, catalog seeding, plan analysis). */
private[connect] object ConnectChannel:

  /** A scoped channel honouring [[TransportConfig]]: acquisition and guaranteed
    * shutdown in one place (Zionomicon ch. 14/15 — the resource cannot leak past
    * the scope). */
  def scoped(
      host: String,
      port: Int,
      config: TransportConfig,
  ): ZIO[Scope, RegistrationError, ManagedChannel] =
    ZIO
      .acquireRelease(ZIO.attemptBlocking(build(host, port, config)))(ch =>
        ZIO.attemptBlocking {
          ch.shutdownNow()
          val _ = ch.awaitTermination(10, TimeUnit.SECONDS)
        }.orDie
      )
      .mapError(e => RegistrationError.TransportFailure(e.toString))

  /** Build the channel from the pure [[TransportConfig.plan]] decision. */
  private def build(host: String, port: Int, config: TransportConfig): ManagedChannel =
    val plan    = TransportConfig.plan(host, port, config)
    val builder = ManagedChannelBuilder.forAddress(plan.host, plan.port)
    plan.security match
      case TransportConfig.Security.Plaintext => builder.usePlaintext()
      case TransportConfig.Security.Tls       => builder.useTransportSecurity()
    // Bearer credentials as an attach-headers interceptor: the token rides on
    // EVERY call on this channel (the DatabricksProbe recipe) and never appears
    // in a log line or an error message.
    config.token.foreach { secret =>
      val md = new Metadata()
      md.put(Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER), s"Bearer $secret")
      builder.intercept(MetadataUtils.newAttachHeadersInterceptor(md))
    }
    builder.build()

  /** Apply the unary deadline to a stub. Server-streaming calls (`StartRun`)
    * deliberately skip this — see [[TransportConfig.DefaultDeadlineSeconds]]. */
  def withDeadline[S <: AbstractStub[S]](stub: S, config: TransportConfig): S =
    stub.withDeadlineAfter(config.deadlineSeconds, TimeUnit.SECONDS)

/** **The seam.** Everything the three Spark Connect clients
  * ([[PipelinesRegistration]], [[CatalogSeeder]], [[PlanAnalysis]]) actually
  * need from a server, and nothing else: send a plan, read the responses back.
  *
  * Why a trait here and nowhere else (P3.2). The ZIO service pattern earns its
  * keep exactly where a dependency is *genuinely* swappable — and this one is:
  * the live implementation is a gRPC channel, and a test's implementation is a
  * script of canned responses. Before this existed, every failure-path decision
  * in the clients (which gRPC status becomes `TransportFailure` vs
  * `ServerRejected`, how a mid-sequence `DefineFlow` rejection aborts the
  * sequence, what a run-stream event turns into) could only be exercised with a
  * container on the other end, which is why almost none of it was. Nothing else
  * in this package gets a trait: the encoders and the version gate are pure
  * functions with one implementation, and wrapping those in accessors would be
  * ceremony.
  *
  * The error channel is deliberately `Throwable`, not [[RegistrationError]]:
  * the *mapping* from a gRPC status to a typed verdict is the thing under test,
  * so it must live in the client (`PipelinesRegistration.grpcError`,
  * `PlanAnalysis.analysisError`, `CatalogSeeder`'s per-statement mapping), not
  * behind the seam where a stub would have to fake it.
  *
  * Responses arrive as a `ZStream` rather than a materialized `List` because
  * that is what gRPC gives us (a blocking iterator) and because the partial
  * responses seen *before* a failure are the error's best context — a caller
  * can accumulate as it pulls. Zionomicon ch. 36: server streaming = `ZStream`.
  */
private[connect] trait ConnectTransport:

  /** `sc://host:port` — for log lines and handshake messages only. */
  def endpoint: String

  /** Send a plan whose execution is *metadata work*: registration commands and
    * seed SQL. The per-RPC deadline from [[TransportConfig]] applies, so a
    * wedged server cannot hang a build forever. */
  def execute(request: sc.ExecutePlanRequest): ZStream[Any, Throwable, sc.ExecutePlanResponse]

  /** Send a plan whose execution is *the run itself* (`StartRun`). Deliberately
    * NOT deadlined: a real run legitimately takes minutes, and the caller
    * bounds it by racing the drain against [[cancel]]. */
  def executeUnbounded(request: sc.ExecutePlanRequest): ZStream[Any, Throwable, sc.ExecutePlanResponse]

  /** One `AnalyzePlan` round trip (schema inference, the version handshake). */
  def analyze(request: sc.AnalyzePlanRequest): IO[Throwable, sc.AnalyzePlanResponse]

  /** Force-close the transport, unblocking a pull parked in a never-terminating
    * run so a timeout can detach (ch. 8 — interruption waits for finalizers). */
  def cancel: UIO[Unit]

private[connect] object ConnectTransport:

  /** The live transport over a scoped [[ConnectChannel]], as an effect.
    *
    * `ZIO[Scope, …]` and not a plain value because the channel is a resource;
    * `Scope` in the *caller's* environment and not inside this effect because
    * [[PipelinesRegistration.register]] hands back a `RunHandle` whose stream is
    * consumed after `register` returns — the channel must outlive the effect
    * that built it, which is precisely what a caller-owned scope expresses.
    */
  def scoped(
      host: String,
      port: Int,
      config: TransportConfig,
  ): ZIO[Scope, RegistrationError, ConnectTransport] =
    ConnectChannel.scoped(host, port, config).map(new ChannelTransport(_, config, s"sc://$host:$port"))

  /** The same thing as a layer, for the callers whose whole interaction fits
    * inside one effect (`CatalogSeeder.run`, `PlanAnalysis.analyzeSchema`):
    * there the resource's lifetime IS the effect's, so `.provide` closing the
    * layer's scope on completion is exactly right. */
  def live(
      host: String,
      port: Int,
      config: TransportConfig,
  ): ZLayer[Any, RegistrationError, ConnectTransport] =
    ZLayer.scoped(scoped(host, port, config))

  /** The gRPC implementation: one channel, one blocking stub, responses pulled
    * one at a time on the blocking pool with `attemptBlockingInterrupt` so an
    * interrupt actually lands on a parked `next()`. */
  private final class ChannelTransport(
      channel: ManagedChannel,
      config: TransportConfig,
      val endpoint: String,
  ) extends ConnectTransport:

    private val stub = sc.SparkConnectServiceGrpc.newBlockingStub(channel)

    def execute(request: sc.ExecutePlanRequest): ZStream[Any, Throwable, sc.ExecutePlanResponse] =
      responses(ConnectChannel.withDeadline(stub, config), request)

    def executeUnbounded(request: sc.ExecutePlanRequest): ZStream[Any, Throwable, sc.ExecutePlanResponse] =
      responses(stub, request)

    def analyze(request: sc.AnalyzePlanRequest): IO[Throwable, sc.AnalyzePlanResponse] =
      ZIO.attemptBlockingInterrupt(ConnectChannel.withDeadline(stub, config).analyzePlan(request))

    def cancel: UIO[Unit] = ZIO.succeed { val _ = channel.shutdownNow() }

    private def responses(
        s: sc.SparkConnectServiceGrpc.SparkConnectServiceBlockingStub,
        request: sc.ExecutePlanRequest,
    ): ZStream[Any, Throwable, sc.ExecutePlanResponse] =
      ZStream
        .fromZIO(ZIO.attemptBlockingInterrupt(s.executePlan(request)))
        .flatMap { it =>
          ZStream.unfoldZIO(it) { i =>
            ZIO.attemptBlockingInterrupt(if i.hasNext then Some((i.next(), i)) else None)
          }
        }
