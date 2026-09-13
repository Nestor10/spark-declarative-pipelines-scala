package dev.sdp.connect.app

/** The argv front-end of [[SdpApp]], as a **pure total function**
  * (`List[String] => Either[CliError, Command]`).
  *
  * Why a parser and not a `contains` check: the previous `run` branch accepted
  * any residual argv and asked `rest.contains("--dry")`, so a typo'd flag
  * (`--dry-run`, `--drt`) silently fell through to a REAL run against the
  * configured endpoint. An unrecognised token is an EXPECTED user error
  * (Zionomicon ch. 3 — expected errors are values), so parsing returns an
  * `Either` that the caller renders and turns into exit code 1; nothing
  * executes until the whole argv has been understood.
  */
object SdpCli:

  /** The fully-parsed intent of an argv. */
  enum Command:
    case Usage
    case Validate
    case Manifest(out: Option[String])
    case Run(dry: Boolean, fullRefresh: Boolean = false)

    /** `dump-wire [--out dir]` — write the registration sequence as protobuf
      * text format. Offline and deterministic (see `WireDump`). */
    case DumpWire(out: Option[String])

    /** `explain [flow] [--formatted]` — ask the server to explain each flow's
      * relation. LIVE (see `PlanExplain`). */
    case Explain(flowName: Option[String], formatted: Boolean)

  /** Everything an argv can get wrong, rendered as one readable line. The
    * caller prints the usage text after it. */
  enum CliError:
    case UnknownCommand(token: String)
    case UnexpectedArg(command: String, token: String)
    case MissingValue(command: String, flag: String)

    /** Two flags that are each fine alone and contradictory together. The
      * `reason` is the shared rule's own sentence (see
      * [[SdpCommands.DryFullRefreshRefusal]]) so this front end and the sbt
      * plugin refuse in the same words. */
    case ConflictingFlags(command: String, reason: String)

    def render: String = this match
      case UnknownCommand(token)       => s"sdp: unknown command '$token'"
      case UnexpectedArg(cmd, token)   => s"sdp: $cmd: unexpected argument '$token'"
      case MissingValue(cmd, flag)     => s"sdp: $cmd: $flag requires a value"
      case ConflictingFlags(cmd, why)  => s"sdp: $cmd: $why"

  /** Parse an argv. Every subcommand rejects tokens it does not know. */
  def parse(args: List[String]): Either[CliError, Command] =
    args match
      case Nil                       => Right(Command.Usage)
      case ("--help" | "-h") :: rest => noExtras("--help", rest, Command.Usage)
      case "validate" :: rest        => noExtras("validate", rest, Command.Validate)
      case "manifest" :: rest        => outArgs("manifest", rest, None, Command.Manifest(_))
      case "dump-wire" :: rest       => outArgs("dump-wire", rest, None, Command.DumpWire(_))
      case "explain" :: rest         => explainArgs(rest, None, formatted = false)
      case "run" :: rest             => runArgs(rest, dry = false, fullRefresh = false)
      case token :: _                => Left(CliError.UnknownCommand(token))

  /** `explain [<flow>] [--formatted]` — at most one flow name, and the one
    * flag. A bare `explain` explains every flow. A second positional token is
    * an error rather than a silently ignored typo: "explain gold silver" must
    * not quietly explain only `gold`. */
  private def explainArgs(
      args: List[String],
      flowName: Option[String],
      formatted: Boolean,
  ): Either[CliError, Command] =
    args match
      case Nil                       => Right(Command.Explain(flowName, formatted))
      case "--formatted" :: rest     => explainArgs(rest, flowName, formatted = true)
      case token :: rest if !token.startsWith("-") && flowName.isEmpty =>
        explainArgs(rest, Some(token), formatted)
      case token :: _ => Left(CliError.UnexpectedArg("explain", token))

  /** `<command> [--out <path> | -o <path>]` — nothing else. Shared by
    * `manifest` (a file) and `dump-wire` (a directory): both take exactly one
    * optional destination, and a near-miss flag must be an error in both, so
    * there is one parser rather than two that can drift. */
  private def outArgs(
      command: String,
      args: List[String],
      out: Option[String],
      make: Option[String] => Command,
  ): Either[CliError, Command] =
    args match
      case Nil => Right(make(out))
      case (flag @ ("--out" | "-o")) :: rest =>
        rest match
          case path :: tail if !path.startsWith("-") => outArgs(command, tail, Some(path), make)
          case _                                     => Left(CliError.MissingValue(command, flag))
      case token :: _ => Left(CliError.UnexpectedArg(command, token))

  /** `run [--dry] [--full-refresh]` — nothing else. A near-miss like
    * `--dry-run` is an error, never a silent real run; and the one combination
    * that means nothing (`--dry --full-refresh`) is refused HERE, at parse time,
    * so no environment is read and no channel is opened. Flags may repeat and
    * may appear in either order — they are idempotent statements of intent, and
    * `--full-refresh --dry` must fail exactly like `--dry --full-refresh`, which
    * is why the conflict is checked once, at the end. */
  private def runArgs(
      args: List[String],
      dry: Boolean,
      fullRefresh: Boolean,
  ): Either[CliError, Command] =
    args match
      case Nil =>
        SdpCommands
          .checkRunMode(dry, fullRefresh)
          .left
          .map(CliError.ConflictingFlags("run", _))
          .map(_ => Command.Run(dry, fullRefresh))
      case "--dry" :: rest          => runArgs(rest, dry = true, fullRefresh)
      case "--full-refresh" :: rest => runArgs(rest, dry, fullRefresh = true)
      case token :: _               => Left(CliError.UnexpectedArg("run", token))

  private def noExtras(
      command: String,
      rest: List[String],
      command0: => Command,
  ): Either[CliError, Command] =
    rest match
      case Nil        => Right(command0)
      case token :: _ => Left(CliError.UnexpectedArg(command, token))
