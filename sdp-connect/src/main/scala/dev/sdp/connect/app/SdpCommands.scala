package dev.sdp.connect.app

import dev.sdp.app.{GraphValidation, ManifestAssembly}
import dev.sdp.connect.PipelinesRegistration
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

  /** Effective configuration for a run, resolved from the environment. */
  final case class RunConfig(host: String, port: Int, storage: String)

  /** Expected, renderable failures of a subcommand. */
  enum CommandError:
    case Invalid(errors: ::[PipelineValidationError])
    case Registration(error: PipelinesRegistration.RegistrationError)

    /** Multi-line, human-readable rendering for the console. */
    def render: String = this match
      case Invalid(errors)      => ValidationRendering.invalidGraphMessage(errors.toList)
      case Registration(error)  => s"sdp: registration failed — ${error.describe}"

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

  /** `run`: validate, then register the graph over Spark Connect and drain
    * the run's progress stream (logging each event). `dry = true` maps to the
    * server's validate-only StartRun. The channel is a scoped resource —
    * acquired here, guaranteed shutdown on exit/interrupt
    * (`PipelinesRegistration.register` owns the acquireRelease). */
  def run(
      pipeline: List[GraphFragment],
      config: RunConfig,
      dry: Boolean,
  ): IO[CommandError, String] =
    for
      manifest <- assemble(pipeline)
      graphId <- ZIO
        .scoped {
          PipelinesRegistration
            .register(config.host, config.port, manifest, config.storage, dry)
            .flatMap { handle =>
              handle.progress
                .tap(p => Console.printLine(s"sdp:   • ${p.raw}").orDie)
                .runDrain
                .as(handle.graphId)
            }
        }
        .mapError(CommandError.Registration(_))
    yield graphId
