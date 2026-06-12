package dev.sdp.connect.app

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}

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

  private val DefaultEndpoint = "sc://localhost:15002"

  /** Resolve the effective pipeline name (env override > [[name]]). */
  private val effectiveName: UIO[String] =
    env(NameVar).map(_.filter(_.nonEmpty).getOrElse(name))

  /** Build the run configuration purely from the environment. */
  private val runConfig: UIO[SdpCommands.RunConfig] =
    for
      endpoint <- env(EndpointVar).map(_.filter(_.nonEmpty).getOrElse(DefaultEndpoint))
      nm       <- effectiveName
      (host, port) = parseEndpoint(endpoint)
      storageOpt <- env(StorageVar)
      storage = storageOpt.filter(_.nonEmpty).getOrElse(s"file:///tmp/sdp/$nm")
      defaultCatalog  <- env(DefaultCatalogVar).map(_.filter(_.nonEmpty))
      defaultDatabase <- env(DefaultDatabaseVar).map(_.filter(_.nonEmpty))
    yield SdpCommands.RunConfig(host, port, storage, defaultCatalog, defaultDatabase)

  /** Read one environment variable (12factor: config from env). */
  private def env(key: String): UIO[Option[String]] =
    ZIO.succeed(Option(java.lang.System.getenv(key)))

  /** `sc://host:port` → (host, port); a malformed endpoint is a config
    * defect — die loudly, the operator must fix the env. */
  private def parseEndpoint(endpoint: String): (String, Int) =
    endpoint match
      case s"sc://$host:$port" if port.toIntOption.isDefined => (host, port.toInt)
      case other =>
        throw new IllegalArgumentException(
          s"$EndpointVar must look like sc://host:port, got '$other'"
        )

  // -------------------------------------------------------------------
  // entry point
  // -------------------------------------------------------------------

  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    getArgs.map(_.toList).flatMap(dispatch)

  /** Map parsed args to a subcommand effect, then to an exit code. */
  private def dispatch(args: List[String]): URIO[Any, ExitCode] =
    args match
      case Nil                          => printUsage.as(ExitCode.success)
      case "--help" :: _ | "-h" :: _    => printUsage.as(ExitCode.success)
      case "validate" :: _              => finish(SdpCommands.validate(pipeline))
      case "manifest" :: rest           => finish(manifestCmd(parseOut(rest)))
      case "run" :: rest                => finish(runCmd(rest.contains("--dry")))
      case other :: _ =>
        (Console.printLineError(s"sdp: unknown command '$other'").orDie *> printUsage)
          .as(ExitCode.failure)

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

  /** `run`: resolve env config, register + run, report the graph id. */
  private def runCmd(dry: Boolean): IO[SdpCommands.CommandError, Unit] =
    for
      config  <- runConfig
      verb     = if dry then "validating" else "running"
      _       <- Console.printLine(s"sdp: $verb pipeline on sc://${config.host}:${config.port} (dry=$dry, storage=${config.storage})").orDie
      graphId <- SdpCommands.run(pipeline, config, dry)
      mode     = if dry then "validated (dry run)" else "executed"
      _       <- Console.printLine(s"sdp: pipeline $mode; dataflow graph id: $graphId").orDie
    yield ()

  /** Render expected failures and translate to an exit code; defects still
    * die. A success exits 0, a `CommandError` prints + exits 1. */
  private def finish(effect: IO[SdpCommands.CommandError, Unit]): URIO[Any, ExitCode] =
    effect.foldZIO(
      err => Console.printLineError(err.render).orDie.as(ExitCode.failure),
      _ => ZIO.succeed(ExitCode.success),
    )

  /** `--out <path>` (or `-o <path>`) from the residual args. */
  private def parseOut(args: List[String]): Option[String] =
    args match
      case ("--out" | "-o") :: path :: _ => Some(path)
      case _ :: rest                     => parseOut(rest)
      case Nil                           => None

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
           |
           |validate and manifest are offline and need no environment.""".stripMargin
      )
      .orDie
