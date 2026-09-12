package dev.sdp.connect.app

import dev.sdp.app.{GraphValidation, ManifestAssembly}
import dev.sdp.connect.{PipelinesRegistration, TransportConfig}
import dev.sdp.core.{GraphFragment, PipelineManifest, PipelineValidationError}
import zio.*

/** The `SdpApp` subcommand bodies, as plain ZIO values — deliberately
  * decoupled from `getArgs`, process exit, and stdout-vs-file choice so they
  * are testable as effects (Task 3: test the effect, not the process).
  *
  * Error model (Zionomicon ch. 3): the EXPECTED failure of assembling a
  * pipeline is a non-empty list of [[PipelineValidationError]] — it lives in
  * the typed error channel as [[CommandError.Invalid]], rendered with the same
  * words the sbt plugin uses (see [[ValidationRendering]]). Spark Connect
  * transport/server rejections are [[CommandError.Registration]]. Codec and
  * network defects die loudly.
  */
object SdpCommands:

  /** Effective configuration for a run, resolved from the environment.
    *
    * `transport` carries TLS/bearer/deadline and redacts its token in
    * `toString`, so echoing a `RunConfig` can never leak the credential. */
  final case class RunConfig(
      host: String,
      port: Int,
      storage: String,
      // Graph defaults — the dev/prod environment switch (see SdpApp env vars).
      defaultCatalog: Option[String] = None,
      defaultDatabase: Option[String] = None,
      transport: TransportConfig = TransportConfig.plaintext,
      /** Seconds to wait for the run before detaching (the drain-vs-cancel
        * race below). A batch or terminating-streaming run finishes well
        * inside it; an unbounded source hits it and detaches cleanly instead
        * of wedging the process. */
      runTimeoutSeconds: Long = 600L,
  )

  /** Expected, renderable failures of a subcommand. */
  enum CommandError:
    case Invalid(errors: ::[PipelineValidationError])
    case Registration(error: PipelinesRegistration.RegistrationError)

    /** The environment is misconfigured (e.g. a malformed
      * `SDP_CONNECT_ENDPOINT`). An operator mistake, not a defect — one
      * readable line plus the usage text, never a fiber dump. */
    case BadConfig(detail: String)

    /** Multi-line, human-readable rendering for the console. */
    def render: String = this match
      case Invalid(errors)     => ValidationRendering.invalidGraphMessage(errors.toList)
      case Registration(error) => s"sdp: registration failed — ${error.describe}"
      case BadConfig(detail)   => s"sdp: $detail"

    /** Config mistakes are usage mistakes: the caller reprints the usage
      * text after [[render]]. Graph/server verdicts stand on their own. */
    def showUsage: Boolean = this match
      case BadConfig(_) => true
      case _            => false

  /** `sc://host:port` → `(host, port)`. A malformed endpoint is an EXPECTED
    * config failure in the typed channel (`BadConfig`) — the operator fixes
    * the env var — not an `IllegalArgumentException` defect.
    *
    * @param envVar the variable (or setting) name to name in the message
    */
  def parseEndpoint(envVar: String, endpoint: String): Either[CommandError, (String, Int)] =
    endpoint match
      case s"sc://$host:$port"
          if host.nonEmpty && port.toIntOption.exists(p => p > 0 && p <= 65535) =>
        Right((host, port.toInt))
      case other =>
        Left(CommandError.BadConfig(s"$envVar must look like sc://host:port, got '$other'"))

  /** A positive number of seconds from a raw config value; unset/empty keeps
    * `default`. Pure and total — a bad value is a `BadConfig`, not a throw. */
  def parsePositiveSeconds(
      envVar: String,
      raw: Option[String],
      default: Long,
  ): Either[CommandError, Long] =
    raw.map(_.trim).filter(_.nonEmpty) match
      case None => Right(default)
      case Some(value) =>
        value.toLongOption.filter(_ > 0) match
          case Some(n) => Right(n)
          case None =>
            Left(
              CommandError.BadConfig(s"$envVar must be a positive number of seconds, got '$value'")
            )

  /** Assemble + validate the fragments into the canonical manifest. The
    * EXPECTED failure (a cycle, dangling read, unknown column, ...) surfaces
    * as `CommandError.Invalid`. Offline — needs no env, no server. */
  def assemble(
      pipeline: List[GraphFragment]
  ): IO[CommandError, PipelineManifest] =
    ManifestAssembly
      .assemble(pipeline)
      .provide(ManifestAssembly.live, GraphValidation.live)
      .mapError(CommandError.Invalid(_))

  /** `validate`: assemble + validate; the manifest is discarded — success is
    * the signal. Offline. */
  def validate(pipeline: List[GraphFragment]): IO[CommandError, Unit] =
    assemble(pipeline).unit

  /** `manifest`: validate, then render the `.sdpm` manifest text. Offline —
    * the caller decides whether to print it or write a file. */
  def manifest(pipeline: List[GraphFragment]): IO[CommandError, String] =
    assemble(pipeline).map(_.render)

  /** The outcome of a run: the graph id, and whether the run actually finished
    * (false = the drain lost the race against the timeout and we detached; the
    * server keeps going). */
  final case class RunOutcome(graphId: String, completed: Boolean)

  /** `run`: validate, then register the graph over Spark Connect and drain
    * the run's progress stream (logging each event). `dry = true` maps to the
    * server's validate-only StartRun. The channel is a scoped resource —
    * acquired here, guaranteed shutdown on exit/interrupt
    * (`PipelinesRegistration.register` owns the acquireRelease).
    *
    * The drain is RACED against `sleep(timeout) *> handle.cancel`, exactly as
    * the `RunHandle` scaladoc instructs: a plain `.timeout` can park waiting on
    * a blocking gRPC pull, so the loser of the race force-closes the channel and
    * the drain unblocks (Zionomicon ch. 8 — interruption waits for finalizers).
    */
  def run(
      pipeline: List[GraphFragment],
      config: RunConfig,
      dry: Boolean,
  ): IO[CommandError, RunOutcome] =
    for
      manifest <- assemble(pipeline)
      outcome <- ZIO
        .scoped {
          PipelinesRegistration
            .register(
              config.host,
              config.port,
              manifest,
              config.storage,
              dry,
              defaultCatalog = config.defaultCatalog,
              defaultDatabase = config.defaultDatabase,
              transport = config.transport,
            )
            .flatMap { handle =>
              val drain = handle.progress
                .tap(p => Console.printLine(s"sdp:   • ${p.raw}").orDie)
                .runDrain
                .as(true)
              val detach =
                ZIO.sleep(Duration.fromSeconds(config.runTimeoutSeconds)) *> handle.cancel.as(false)
              drain.raceFirst(detach).map(RunOutcome(handle.graphId, _))
            }
        }
        .mapError(CommandError.Registration(_))
    yield outcome
