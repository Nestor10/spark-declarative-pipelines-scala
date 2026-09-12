package dev.sdp.connect.app

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}

import dev.sdp.connect.TransportConfig
import dev.sdp.core.GraphFragment
import zio.*

/** The library-first SDP runner (D10).
  *
  * A user mixes this into a single object in their uber jar and implements
  * exactly one member — `def pipeline` — then the jar IS the production
  * runner: Argo/K8s schedules `java -jar app.jar run`, no sbt on the path.
  * The sbt plugin is now just a dev-loop convenience over the same code.
  *
  * Subcommands (read from `getArgs`):
  *   - `validate`           — assemble + validate the graph; exit 0 on a
  *                            sound graph, 1 with the rendered errors otherwise.
  *                            Offline (no env, no server).
  *   - `manifest [--out p]` — validate, then write the `.sdpm` manifest to `p`
  *                            (stdout when `--out` is omitted). Offline.
  *   - `run [--dry]`        — validate, register the graph over Spark Connect,
  *                            and start a run (`--dry` = server-side validation
  *                            only). Reads the environment for the endpoint.
  *   - (no args | --help)   — usage text.
  *
  * The argv is parsed strictly ([[SdpCli]]): an unknown command, an unknown
  * flag, or a near-miss like `run --dry-run` prints the offending token plus
  * the usage text and exits 1. A typo never degrades into a real run.
  *
  * Configuration is **environment-only** (12factor III — config in env):
  *   - `SDP_CONNECT_ENDPOINT`  Spark Connect endpoint `sc://host:port`
  *                             (default `sc://localhost:15002`, the dev container).
  *   - `SDP_STORAGE_ROOT`      pipeline checkpoint/metadata root, an absolute
  *                             URI with a scheme (default `file:///tmp/sdp/<name>`).
  *   - `SDP_PIPELINE_NAME`     overrides [[name]] (the dataflow graph name).
  *   - `SDP_DEFAULT_CATALOG`   graph default catalog (CreateDataflowGraph field 1).
  *   - `SDP_DEFAULT_DATABASE`  graph default database (field 2) — the dev/prod
  *                             switch: unqualified dataset names land here
  *                             (`dev_eric` locally, the real schema in prod).
  *                             Unset = omit. `run` only.
  *   - `SDP_CONNECT_USE_TLS`   `true` to speak TLS instead of plaintext
  *                             (default plaintext — the local dev container).
  *   - `SDP_CONNECT_TOKEN`     bearer token attached to every call. A secret:
  *                             never logged, and redacted in `toString`.
  *   - `SDP_CONNECT_DEADLINE`  per-RPC deadline (seconds) for the registration
  *                             calls; the run stream is bounded instead by
  *                             `SDP_RUN_TIMEOUT`.
  *   - `SDP_RUN_TIMEOUT`       seconds to wait for a run before detaching.
  *
  * `validate` and `manifest` are offline and run with NO env vars set.
  */
trait SdpApp extends ZIOAppDefault:

  /** Type alias for the one member a user implements. */
  final type Pipeline = List[GraphFragment]

  /** The fragments that make up this pipeline — the single thing a user
    * supplies (typically the DSL `table`/`streamingTable`/`view` values). */
  def pipeline: Pipeline

  /** The dataflow graph / pipeline name. Overridable; `SDP_PIPELINE_NAME`
    * wins at runtime when set. */
  def name: String = "sdp-pipeline"

  // -------------------------------------------------------------------
  // env-only configuration (12factor III)
  // -------------------------------------------------------------------

  private val EndpointVar = "SDP_CONNECT_ENDPOINT"
  private val StorageVar  = "SDP_STORAGE_ROOT"
  private val NameVar     = "SDP_PIPELINE_NAME"
  // Graph defaults (CreateDataflowGraph fields 1/2) — the dev/prod switch:
  // code keeps unqualified dataset names, the environment decides where they
  // land (e.g. SDP_DEFAULT_DATABASE=dev_eric locally, =analytics in prod).
  // Unset/empty = omit (server falls back to the session). `run` only;
  // validate/manifest are offline and never read them.
  private val DefaultCatalogVar  = "SDP_DEFAULT_CATALOG"
  private val DefaultDatabaseVar = "SDP_DEFAULT_DATABASE"
  // Transport: TLS + bearer token for a managed endpoint. Unset = plaintext,
  // anonymous — sc://localhost is the dev container and stays untouched. The
  // token is a secret: it is never logged, and `TransportConfig.toString`
  // redacts it so it cannot leak through config echoing.
  private val UseTlsVar   = "SDP_CONNECT_USE_TLS"
  private val TokenVar    = "SDP_CONNECT_TOKEN"
  private val DeadlineVar = "SDP_CONNECT_DEADLINE"
  private val RunTimeoutVar = "SDP_RUN_TIMEOUT"

  private val DefaultEndpoint = "sc://localhost:15002"

  /** Resolve the effective pipeline name (env override > [[name]]). */
  private val effectiveName: UIO[String] =
    env(NameVar).map(_.filter(_.nonEmpty).getOrElse(name))

  /** Build the run configuration purely from the environment. A malformed
    * endpoint fails in the typed channel (`CommandError.BadConfig`), so the
    * operator reads one line plus the usage text. */
  private val runConfig: IO[SdpCommands.CommandError, SdpCommands.RunConfig] =
    for
      endpoint <- env(EndpointVar).map(_.filter(_.nonEmpty).getOrElse(DefaultEndpoint))
      nm       <- effectiveName
      hostPort <- ZIO.fromEither(SdpCommands.parseEndpoint(EndpointVar, endpoint))
      (host, port) = hostPort
      storageOpt <- env(StorageVar)
      storage = storageOpt.filter(_.nonEmpty).getOrElse(s"file:///tmp/sdp/$nm")
      defaultCatalog  <- env(DefaultCatalogVar).map(_.filter(_.nonEmpty))
      defaultDatabase <- env(DefaultDatabaseVar).map(_.filter(_.nonEmpty))
      useTls          <- env(UseTlsVar)
      token           <- env(TokenVar)
      deadline        <- env(DeadlineVar)
      transport <- ZIO.fromEither(
        TransportConfig
          .parse(useTls, token, deadline, tlsVarName = UseTlsVar, deadlineVarName = DeadlineVar)
          .left
          .map(SdpCommands.CommandError.BadConfig(_))
      )
      runTimeoutRaw <- env(RunTimeoutVar)
      runTimeout <- ZIO.fromEither(
        SdpCommands.parsePositiveSeconds(RunTimeoutVar, runTimeoutRaw, default = 600L)
      )
    yield SdpCommands.RunConfig(
      host,
      port,
      storage,
      defaultCatalog,
      defaultDatabase,
      transport,
      runTimeout,
    )

  /** Read one environment variable (12factor: config from env). Goes through
    * the ZIO `System` service so tests can stub the environment. */
  private def env(key: String): UIO[Option[String]] =
    zio.System.env(key).orDie

  // -------------------------------------------------------------------
  // entry point
  // -------------------------------------------------------------------

  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    getArgs.map(_.toList).flatMap(dispatch)

  /** Map an argv to a subcommand effect, then to an exit code. Parsing is the
    * pure [[SdpCli.parse]]; an unrecognised command or flag prints the
    * offending token plus the usage text and exits 1 — nothing runs.
    *
    * `private[app]` so the spec can drive it directly (the dispatch table,
    * not the JVM process, is what must be tested). */
  private[app] def dispatch(args: List[String]): URIO[Any, ExitCode] =
    SdpCli.parse(args) match
      case Left(err) =>
        (Console.printLineError(err.render).orDie *> printUsage).as(ExitCode.failure)
      case Right(SdpCli.Command.Usage)          => printUsage.as(ExitCode.success)
      case Right(SdpCli.Command.Validate)       => finish(SdpCommands.validate(pipeline))
      case Right(SdpCli.Command.Manifest(out))  => finish(manifestCmd(out))
      case Right(SdpCli.Command.Run(dry))       => finish(runCmd(dry))

  /** `manifest`: render and either write to `out` or print to stdout. */
  private def manifestCmd(out: Option[String]): IO[SdpCommands.CommandError, Unit] =
    SdpCommands.manifest(pipeline).flatMap { text =>
      out match
        case Some(path) =>
          ZIO.attemptBlocking {
            val p = Paths.get(path)
            Option(p.getParent).foreach(Files.createDirectories(_))
            Files.write(p, text.getBytes(UTF_8))
          }.orDie *> Console.printLine(s"sdp: wrote manifest to $path").orDie
        case None =>
          Console.printLine(text).orDie
    }

  /** `run`: resolve env config, register + run, report the graph id. The log
    * line names the transport mode but NEVER the token. */
  private def runCmd(dry: Boolean): IO[SdpCommands.CommandError, Unit] =
    for
      config <- runConfig
      verb    = if dry then "validating" else "running"
      mode    = if config.transport.useTls then "tls" else "plaintext"
      auth    = if config.transport.token.isDefined then ", bearer token" else ""
      _ <- Console
        .printLine(
          s"sdp: $verb pipeline on sc://${config.host}:${config.port} " +
            s"($mode$auth, dry=$dry, storage=${config.storage})"
        )
        .orDie
      outcome <- SdpCommands.run(pipeline, config, dry)
      _ <-
        if outcome.completed then
          val what = if dry then "validated (dry run)" else "executed"
          Console.printLine(s"sdp: pipeline $what; dataflow graph id: ${outcome.graphId}").orDie
        else
          Console
            .printLineError(
              s"sdp: run still in progress after ${config.runTimeoutSeconds}s — detached. The " +
                s"server keeps running; raise $RunTimeoutVar or use a terminating source for a " +
                s"one-shot run. (graph id: ${outcome.graphId})"
            )
            .orDie
    yield ()

  /** Render expected failures and translate to an exit code; defects still
    * die. A success exits 0, a `CommandError` prints + exits 1 — and a config
    * mistake reprints the usage text. */
  private def finish(effect: IO[SdpCommands.CommandError, Unit]): URIO[Any, ExitCode] =
    effect.foldZIO(
      err =>
        (Console.printLineError(err.render).orDie *> ZIO.when(err.showUsage)(printUsage))
          .as(ExitCode.failure),
      _ => ZIO.succeed(ExitCode.success),
    )

  private val printUsage: UIO[Unit] =
    Console
      .printLine(
        s"""sdp — Spark Declarative Pipelines runner ($name)
           |
           |Usage: <app> <command> [options]
           |
           |Commands:
           |  validate            Assemble and validate the pipeline graph. Offline.
           |  manifest [--out p]  Validate, then write the .sdpm manifest to p
           |                      (stdout if --out is omitted). Offline.
           |  run [--dry]         Validate, register the graph over Spark Connect,
           |                      and start a run. --dry = server-side validation only.
           |  --help              Show this message.
           |
           |Environment (config is read from the environment — 12factor III):
           |  $EndpointVar   Spark Connect endpoint sc://host:port
           |                          (default $DefaultEndpoint).
           |  $StorageVar       Checkpoint/metadata root, an absolute URI with a
           |                          scheme (default file:///tmp/sdp/<name>).
           |  $NameVar      Override the pipeline / dataflow graph name.
           |  $DefaultCatalogVar     Graph default catalog (run only; unset = omit).
           |  $DefaultDatabaseVar    Graph default database — the dev/prod switch:
           |                          unqualified dataset names land here
           |                          (dev_eric locally, the real schema in prod).
           |  $UseTlsVar    true to use TLS (default false = plaintext, the
           |                          local dev container).
           |  $TokenVar     Bearer token sent on every call (never logged).
           |  $DeadlineVar  Per-RPC deadline in seconds for registration calls
           |                          (default ${TransportConfig.DefaultDeadlineSeconds}).
           |  $RunTimeoutVar         Seconds to wait for a run before detaching
           |                          (default 600).
           |
           |validate and manifest are offline and need no environment.""".stripMargin
      )
      .orDie
