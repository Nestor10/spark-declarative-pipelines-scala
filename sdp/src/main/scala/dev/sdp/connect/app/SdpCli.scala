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
    case Run(dry: Boolean)

  /** Everything an argv can get wrong, rendered as one readable line. The
    * caller prints the usage text after it. */
  enum CliError:
    case UnknownCommand(token: String)
    case UnexpectedArg(command: String, token: String)
    case MissingValue(command: String, flag: String)

    def render: String = this match
      case UnknownCommand(token)       => s"sdp: unknown command '$token'"
      case UnexpectedArg(cmd, token)   => s"sdp: $cmd: unexpected argument '$token'"
      case MissingValue(cmd, flag)     => s"sdp: $cmd: $flag requires a value"

  /** Parse an argv. Every subcommand rejects tokens it does not know. */
  def parse(args: List[String]): Either[CliError, Command] =
    args match
      case Nil                       => Right(Command.Usage)
      case ("--help" | "-h") :: rest => noExtras("--help", rest, Command.Usage)
      case "validate" :: rest        => noExtras("validate", rest, Command.Validate)
      case "manifest" :: rest        => manifestArgs(rest, None)
      case "run" :: rest             => runArgs(rest, dry = false)
      case token :: _                => Left(CliError.UnknownCommand(token))

  /** `manifest [--out <path> | -o <path>]` — nothing else. */
  private def manifestArgs(args: List[String], out: Option[String]): Either[CliError, Command] =
    args match
      case Nil => Right(Command.Manifest(out))
      case (flag @ ("--out" | "-o")) :: rest =>
        rest match
          case path :: tail if !path.startsWith("-") => manifestArgs(tail, Some(path))
          case _                                     => Left(CliError.MissingValue("manifest", flag))
      case token :: _ => Left(CliError.UnexpectedArg("manifest", token))

  /** `run [--dry]` — nothing else. A near-miss like `--dry-run` is an error,
    * never a silent real run. */
  private def runArgs(args: List[String], dry: Boolean): Either[CliError, Command] =
    args match
      case Nil             => Right(Command.Run(dry))
      case "--dry" :: rest => runArgs(rest, dry = true)
      case token :: _      => Left(CliError.UnexpectedArg("run", token))

  private def noExtras(
      command: String,
      rest: List[String],
      command0: => Command,
  ): Either[CliError, Command] =
    rest match
      case Nil        => Right(command0)
      case token :: _ => Left(CliError.UnexpectedArg(command, token))
