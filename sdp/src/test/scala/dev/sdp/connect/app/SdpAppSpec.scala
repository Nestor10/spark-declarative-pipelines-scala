package dev.sdp.connect.app

import dev.sdp.core.*
import zio.*
import zio.test.*

/** The `SdpApp` surface, tested as ZIO values (no JVM process, no container):
  * argv parsing, the dispatch table's rendered output and exit codes, and the
  * offline subcommand logic. A valid pipeline assembles; a pipeline with a
  * dangling read fails in the typed channel with the same error rendering the
  * sbt plugin emits (Zionomicon ch. 3: expected errors are values).
  *
  * Fragments are built straight from the core ADTs (the DSL builds the same
  * `GraphFragment` shapes), so the test needs nothing but `dev.sdp.core`.
  */
object SdpAppSpec extends ZIOSpecDefault:

  /** Two-fragment valid pipeline: a source table and a streaming table that
    * reads it. The edge resolves because both endpoints are declared. */
  private val validPipeline: List[GraphFragment] = List(
    GraphFragment(List(PipelineNode.Table("bronze", "delta")), Set.empty),
    GraphFragment(
      List(PipelineNode.StreamingTable("silver", "delta")),
      Set(DependencyEdge("bronze", "silver")),
    ),
  )

  /** A pipeline whose lineage references an undeclared upstream ("ghost") —
    * the validator must surface a DanglingEdges error. */
  private val danglingPipeline: List[GraphFragment] = List(
    GraphFragment(
      List(PipelineNode.StreamingTable("silver", "delta")),
      Set(DependencyEdge("ghost", "silver")),
    )
  )

  /** A real `SdpApp` over the valid pipeline — instantiated, never `main`ed, so
    * the dispatch table can be driven as an effect. */
  private object TestApp extends SdpApp:
    override def name: String        = "spec-pipeline"
    def pipeline: List[GraphFragment] = validPipeline

  def spec = suite("SdpApp offline command logic")(
    test("validate: a sound 2-fragment pipeline succeeds") {
      SdpCommands.validate(validPipeline).exit.map { exit =>
        assertTrue(exit.isSuccess)
      }
    },
    test("manifest: a sound pipeline renders a parseable .sdpm manifest") {
      for
        text <- SdpCommands.manifest(validPipeline)
      yield assertTrue(
        text.startsWith("sdp-manifest/"),
        PipelineManifest.parse(text).isRight,
      )
    },
    test("validate: a dangling read fails with the rendered DanglingEdges error") {
      SdpCommands.validate(danglingPipeline).either.map {
        case Left(err: SdpCommands.CommandError.Invalid) =>
          val rendered = err.render
          assertTrue(
            err.errors.exists {
              case PipelineValidationError.DanglingEdges(ids) => ids.contains("ghost")
              case _                                          => false
            },
            rendered.contains("SDP pipeline graph is invalid"),
            rendered.contains("ghost"),
          )
        case other =>
          assertTrue(false) ?? s"expected CommandError.Invalid, got $other"
      }
    },
    suite("argv parsing (SdpCli)")(
      test("run --dry is accepted as a dry run; a bare run is a real run") {
        assertTrue(
          SdpCli.parse(List("run", "--dry")) == Right(SdpCli.Command.Run(dry = true)),
          SdpCli.parse(List("run")) == Right(SdpCli.Command.Run(dry = false)),
        )
      },
      test("a typo'd --dry is rejected, never silently a real run") {
        assertTrue(
          SdpCli.parse(List("run", "--dry-run")) ==
            Left(SdpCli.CliError.UnexpectedArg("run", "--dry-run")),
          SdpCli.parse(List("run", "--drt")) ==
            Left(SdpCli.CliError.UnexpectedArg("run", "--drt")),
        )
      },
      test("manifest --out takes a path; a bare --out is a missing value") {
        assertTrue(
          SdpCli.parse(List("manifest")) == Right(SdpCli.Command.Manifest(None)),
          SdpCli.parse(List("manifest", "--out", "t/p.sdpm")) ==
            Right(SdpCli.Command.Manifest(Some("t/p.sdpm"))),
          SdpCli.parse(List("manifest", "--out")) ==
            Left(SdpCli.CliError.MissingValue("manifest", "--out")),
          SdpCli.parse(List("manifest", "--quiet")) ==
            Left(SdpCli.CliError.UnexpectedArg("manifest", "--quiet")),
        )
      },
      test("validate takes no arguments") {
        assertTrue(
          SdpCli.parse(List("validate")) == Right(SdpCli.Command.Validate),
          SdpCli.parse(List("validate", "--hard")) ==
            Left(SdpCli.CliError.UnexpectedArg("validate", "--hard")),
        )
      },
      test("no args and --help are usage") {
        assertTrue(
          SdpCli.parse(Nil) == Right(SdpCli.Command.Usage),
          SdpCli.parse(List("--help")) == Right(SdpCli.Command.Usage),
          SdpCli.parse(List("-h")) == Right(SdpCli.Command.Usage),
        )
      },
    ),
    suite("dispatch: rendered output + exit code")(
      test("an unknown command prints the token + usage and exits 1") {
        for
          code <- TestApp.dispatch(List("valdate"))
          err  <- TestConsole.outputErr
          out  <- TestConsole.output
        yield assertTrue(
          code == ExitCode.failure,
          err.mkString.contains("unknown command 'valdate'"),
          out.mkString.contains("Usage:"),
        )
      },
      test("an unknown flag on run prints the token + usage and exits 1") {
        for
          code <- TestApp.dispatch(List("run", "--dry-run"))
          err  <- TestConsole.outputErr
          out  <- TestConsole.output
        yield assertTrue(
          code == ExitCode.failure,
          err.mkString.contains("run: unexpected argument '--dry-run'"),
          out.mkString.contains("Usage:"),
        )
      },
      test("validate on a sound pipeline exits 0 and prints no error") {
        for
          code <- TestApp.dispatch(List("validate"))
          err  <- TestConsole.outputErr
        yield assertTrue(code == ExitCode.success, err.isEmpty)
      },
      test("--help exits 0 with the usage text") {
        for
          code <- TestApp.dispatch(List("--help"))
          out  <- TestConsole.output
        yield assertTrue(
          code == ExitCode.success,
          out.mkString.contains("Spark Declarative Pipelines runner (spec-pipeline)"),
        )
      },
      test("a malformed SDP_CONNECT_ENDPOINT renders one line + usage, exits 1") {
        for
          _    <- TestSystem.putEnv("SDP_CONNECT_ENDPOINT", "localhost:15002")
          code <- TestApp.dispatch(List("run", "--dry"))
          err  <- TestConsole.outputErr
          out  <- TestConsole.output
        yield assertTrue(
          code == ExitCode.failure,
          err.mkString.contains(
            "sdp: SDP_CONNECT_ENDPOINT must look like sc://host:port, got 'localhost:15002'"
          ),
          // one readable line, not a fiber dump
          !err.mkString.contains("IllegalArgumentException"),
          out.mkString.contains("Usage:"),
        )
      },
      test("a malformed SDP_CONNECT_USE_TLS renders one line + usage, exits 1") {
        for
          _    <- TestSystem.putEnv("SDP_CONNECT_USE_TLS", "maybe")
          _    <- TestSystem.putEnv("SDP_CONNECT_TOKEN", "super-secret-pat")
          code <- TestApp.dispatch(List("run", "--dry"))
          err  <- TestConsole.outputErr
          out  <- TestConsole.output
        yield assertTrue(
          code == ExitCode.failure,
          err.mkString.contains("SDP_CONNECT_USE_TLS must be true or false, got 'maybe'"),
          out.mkString.contains("Usage:"),
          // the token must never reach stdout or stderr
          !err.mkString.contains("super-secret-pat"),
          !out.mkString.contains("super-secret-pat"),
        )
      },
      test("SDP_RUN_TIMEOUT must be a positive number of seconds") {
        assertTrue(
          SdpCommands.parsePositiveSeconds("SDP_RUN_TIMEOUT", None, default = 600L) == Right(600L),
          SdpCommands.parsePositiveSeconds("SDP_RUN_TIMEOUT", Some(" 90 "), 600L) == Right(90L),
          SdpCommands
            .parsePositiveSeconds("SDP_RUN_TIMEOUT", Some("soon"), 600L)
            .left
            .map(_.render) ==
            Left("sdp: SDP_RUN_TIMEOUT must be a positive number of seconds, got 'soon'"),
          SdpCommands.parsePositiveSeconds("SDP_RUN_TIMEOUT", Some("0"), 600L).isLeft,
        )
      },
      test("SDP_SKIP_VERSION_CHECK defaults to off and accepts the usual spellings") {
        assertTrue(
          // Doing nothing leaves the handshake ON — the safe default.
          SdpCommands.parseFlag("SDP_SKIP_VERSION_CHECK", None, default = false) == Right(false),
          SdpCommands.parseFlag("SDP_SKIP_VERSION_CHECK", Some(""), default = false) == Right(false),
          SdpCommands.parseFlag("SDP_SKIP_VERSION_CHECK", Some("true"), false) == Right(true),
          SdpCommands.parseFlag("SDP_SKIP_VERSION_CHECK", Some(" TRUE "), false) == Right(true),
          SdpCommands.parseFlag("SDP_SKIP_VERSION_CHECK", Some("1"), false) == Right(true),
          SdpCommands.parseFlag("SDP_SKIP_VERSION_CHECK", Some("yes"), false) == Right(true),
          SdpCommands.parseFlag("SDP_SKIP_VERSION_CHECK", Some("off"), false) == Right(false),
          // A typo is a config error, never silently "don't skip" — the operator
          // who wrote it meant something.
          SdpCommands
            .parseFlag("SDP_SKIP_VERSION_CHECK", Some("ja"), false)
            .left
            .map(_.render) ==
            Left("sdp: SDP_SKIP_VERSION_CHECK must be true or false, got 'ja'"),
        )
      },
      test("a malformed SDP_SKIP_VERSION_CHECK renders one line + usage, exits 1") {
        for
          _    <- TestSystem.putEnv("SDP_SKIP_VERSION_CHECK", "sometimes")
          code <- TestApp.dispatch(List("run", "--dry"))
          err  <- TestConsole.outputErr
          out  <- TestConsole.output
        yield assertTrue(
          code == ExitCode.failure,
          err.mkString.contains("SDP_SKIP_VERSION_CHECK must be true or false, got 'sometimes'"),
          out.mkString.contains("Usage:"),
        )
      },
      test("the version check is on by default in a RunConfig, and skipping is visible") {
        val default = SdpCommands.RunConfig("h", 15002, "file:///tmp/x")
        assertTrue(
          default.versionCheck,
          default.copy(versionCheck = false).versionCheck == false,
        )
      },
      test("a too-old server renders as one readable runner line, not a trace") {
        // The whole point of routing the handshake verdict through the existing
        // typed channel: the author sees the same `sdp: …` shape as any other
        // expected failure.
        val manifest = PipelineManifest.fromGraphAndFlows(
          PipelineGraph(Map("dim" -> PipelineNode.StreamingTable("dim", "delta")), Set.empty),
          List(
            Flow(
              "dim_auto_cdc",
              "dim",
              FlowDetails.AutoCdc(
                "bronze.cdc",
                List(dev.sdp.core.algebra.Ex.Col("id")),
                dev.sdp.core.algebra.Ex.Col("seq"),
              ),
            )
          ),
        )
        val rendered = dev.sdp.connect.VersionGate
          .check("4.1.1", manifest, "sc://localhost:15002") match
          case Left(err) => SdpCommands.CommandError.Registration(err).render
          case Right(_)  => "UNEXPECTED: accepted"
        assertTrue(
          rendered ==
            "sdp: registration failed — Spark Connect server is too old for this pipeline: " +
            "AUTO CDC flow (SCD type 1) 'dim_auto_cdc' (target 'dim') needs a Spark 4.2+ server; " +
            "sc://localhost:15002 reports 4.1.1",
          rendered.linesIterator.size == 1,
        )
      },
      test("the usage text documents the handshake escape hatch") {
        for
          _   <- TestApp.dispatch(List("--help"))
          out <- TestConsole.output
        yield assertTrue(out.mkString.contains("SDP_SKIP_VERSION_CHECK"))
      },
      test("a malformed endpoint is a typed BadConfig, not a defect") {
        assertTrue(
          SdpCommands.parseEndpoint("SDP_CONNECT_ENDPOINT", "sc://localhost:15002") ==
            Right(("localhost", 15002)),
          SdpCommands
            .parseEndpoint("SDP_CONNECT_ENDPOINT", "sc://localhost:nope")
            .left
            .map(_.render) == Left(
            "sdp: SDP_CONNECT_ENDPOINT must look like sc://host:port, got 'sc://localhost:nope'"
          ),
          SdpCommands.parseEndpoint("SDP_CONNECT_ENDPOINT", "sc://localhost:99999").isLeft,
          SdpCommands.parseEndpoint("SDP_CONNECT_ENDPOINT", "").isLeft,
        )
      },
    ),
  )
