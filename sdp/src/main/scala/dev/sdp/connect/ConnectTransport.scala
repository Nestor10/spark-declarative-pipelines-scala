package dev.sdp.connect

import java.util.concurrent.TimeUnit

import io.grpc.{ManagedChannel, ManagedChannelBuilder, Metadata}
import io.grpc.stub.{AbstractStub, MetadataUtils}
import zio.*

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
