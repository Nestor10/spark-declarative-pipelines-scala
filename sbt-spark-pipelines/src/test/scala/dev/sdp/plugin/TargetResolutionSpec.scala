package dev.sdp.plugin

import zio.test.*

/** `base settings × named target → ResolvedConnection` (E2's resolution rule) as
  * a pure function: which side wins each field, how a `tokenEnv` is read, and
  * what the failure messages say.
  *
  * The environment arrives as a function, so no test mutates or depends on the
  * real process environment.
  */
object TargetResolutionSpec extends ZIOSpecDefault:

  /** The plugin's own defaults, spelled out. */
  private val base = BaseConnection(
    endpoint = "sc://localhost:15002",
    storageRoot = "file:///tmp/sdp/demo",
    defaultCatalog = "",
    defaultDatabase = "",
    useTls = false,
    token = "",
    deadlineSeconds = 60,
    versionCheck = true,
  )

  private val noEnv: String => Option[String] = _ => None

  private val prod = SdpTarget(
    connectEndpoint = "sc://spark.prod.svc:15002",
    defaultCatalog = Some("warehouse"),
    defaultDatabase = Some("analytics"),
    storageRoot = Some("s3a://lake/sdp/prod"),
    useTls = true,
    tokenEnv = Some("SDP_PROD_TOKEN"),
  )

  def spec = suite("TargetResolution")(
    suite("no target — the flat settings ARE the implicit environment")(
      test("base() maps the settings onto the connection, '' meaning omit") {
        val conn = TargetResolution.base(base)
        assertTrue(
          conn.endpoint == "sc://localhost:15002",
          conn.storageRoot == "file:///tmp/sdp/demo",
          conn.defaultCatalog.isEmpty,
          conn.defaultDatabase.isEmpty,
          !conn.transport.useTls,
          conn.transport.token.isEmpty,
          conn.transport.deadlineSeconds == 60L,
          conn.versionCheck,
          conn.origin == "build settings",
        )
      },
      test("non-empty graph defaults and a token come through") {
        val conn = TargetResolution.base(
          base.copy(defaultCatalog = "warehouse", defaultDatabase = "dev_eric", token = "abc")
        )
        assertTrue(
          conn.defaultCatalog == Some("warehouse"),
          conn.defaultDatabase == Some("dev_eric"),
          conn.transport.token == Some("abc"),
        )
      },
    ),
    suite("a named target overrides the connection-shaped values")(
      test("everything the target states wins") {
        val resolved = TargetResolution.resolve(
          base,
          "prod",
          prod,
          env = { case "SDP_PROD_TOKEN" => Some("pat-xyz"); case _ => None },
        )
        assertTrue(
          resolved.map(_.endpoint) == Right("sc://spark.prod.svc:15002"),
          resolved.map(_.storageRoot) == Right("s3a://lake/sdp/prod"),
          resolved.map(_.defaultCatalog) == Right(Some("warehouse")),
          resolved.map(_.defaultDatabase) == Right(Some("analytics")),
          resolved.map(_.transport.useTls) == Right(true),
          resolved.map(_.transport.token) == Right(Some("pat-xyz")),
          resolved.map(_.origin) == Right("target 'prod'"),
        )
      },
      test("what the target omits falls back to the base settings") {
        val minimal = SdpTarget("sc://stage:15002")
        val resolved = TargetResolution.resolve(
          base.copy(
            defaultCatalog = "warehouse",
            defaultDatabase = "shared",
            storageRoot = "file:///tmp/sdp/demo",
            token = "base-token",
            deadlineSeconds = 90,
            versionCheck = false,
          ),
          "stage",
          minimal,
          noEnv,
        )
        assertTrue(
          resolved.map(_.endpoint) == Right("sc://stage:15002"), // never inherited
          resolved.map(_.storageRoot) == Right("file:///tmp/sdp/demo"),
          resolved.map(_.defaultCatalog) == Right(Some("warehouse")),
          resolved.map(_.defaultDatabase) == Right(Some("shared")),
          resolved.map(_.transport.token) == Right(Some("base-token")),
          resolved.map(_.transport.deadlineSeconds) == Right(90L),
          resolved.map(_.versionCheck) == Right(false),
        )
      },
      test("TLS is a property of the endpoint, so the target always decides it") {
        // A target that moved the endpoint must not inherit "TLS on" from a
        // build that happened to be pointed at a managed server.
        val resolved =
          TargetResolution.resolve(base.copy(useTls = true), "dev", SdpTarget("sc://localhost:15002"), noEnv)
        assertTrue(resolved.map(_.transport.useTls) == Right(false))
      },
      test("deadline and versionCheck override only when stated") {
        val strict =
          SdpTarget("sc://prod:15002", deadlineSeconds = Some(300), versionCheck = Some(false))
        val resolved = TargetResolution.resolve(base, "prod", strict, noEnv)
        assertTrue(
          resolved.map(_.transport.deadlineSeconds) == Right(300L),
          resolved.map(_.versionCheck) == Right(false),
        )
      },
      test("the Argo-reserved fields do not participate in resolution") {
        val withArgo = prod.copy(
          tokenEnv = None,
          schedule = Some("0 3 * * *"),
          namespace = Some("data-prod"),
          runnerImage = Some("ghcr.io/acme/sdp-runner:1"),
        )
        assertTrue(
          TargetResolution.resolve(base, "prod", withArgo, noEnv) ==
            TargetResolution.resolve(base, "prod", withArgo.copy(schedule = None, namespace = None, runnerImage = None), noEnv)
        )
      },
    ),
    suite("tokenEnv — the name is config, the value is not")(
      test("the variable is read at resolution time") {
        val resolved = TargetResolution.resolve(
          base,
          "prod",
          prod,
          env = name => if name == "SDP_PROD_TOKEN" then Some("  pat-xyz  ") else None,
        )
        assertTrue(resolved.map(_.transport.token) == Right(Some("pat-xyz")))
      },
      test("an unset variable is a readable error NAMING the variable") {
        val resolved = TargetResolution.resolve(base, "prod", prod, noEnv)
        val message  = resolved.swap.getOrElse("")
        assertTrue(
          resolved.isLeft,
          message.contains("SDP_PROD_TOKEN"),
          message.contains("target 'prod'"),
          message.contains("never logged"),
        )
      },
      test("an empty variable counts as unset") {
        val resolved =
          TargetResolution.resolve(base, "prod", prod, env = _ => Some("   "))
        assertTrue(resolved.isLeft)
      },
      test("the token value never appears in a log-shaped rendering") {
        val Secret = "pat-super-secret"
        val resolved = TargetResolution
          .resolve(base, "prod", prod, env = _ => Some(Secret))
          .getOrElse(sys.error("expected a resolved connection"))
        assertTrue(
          resolved.transport.token == Some(Secret), // it IS carried
          !resolved.toString.contains(Secret),      // but never printed
          !resolved.transport.toString.contains(Secret),
          resolved.toString.contains("<redacted>"),
        )
      },
    ),
    suite("select — the message is the feature")(
      test("an empty sdpTargets explains how to declare one, with a snippet") {
        val message =
          TargetResolution.select(base, Map.empty, "dev", noEnv, "sdpRunOn").swap.getOrElse("")
        assertTrue(
          message.contains("sdpTargets"),
          message.contains("sdpRunOn"),
          message.contains("SdpTarget.userScopedDev"),
          message.contains("docs/environments.md"),
        )
      },
      test("an unknown target lists what IS available, sorted") {
        val targets = Map(
          "prod"  -> prod,
          "dev"   -> SdpTarget("sc://localhost:15002"),
          "stage" -> SdpTarget("sc://stage:15002"),
        )
        val message =
          TargetResolution.select(base, targets, "produciton", noEnv, "sdpDryRunOn").swap.getOrElse("")
        assertTrue(
          message.contains("unknown target 'produciton'"),
          message.contains("dev, prod, stage"),
        )
      },
      test("a known target resolves, and surrounding whitespace is tolerated") {
        val targets = Map("dev" -> SdpTarget("sc://localhost:15002"))
        assertTrue(
          TargetResolution.select(base, targets, " dev ", noEnv, "sdpRunOn").map(_.endpoint) ==
            Right("sc://localhost:15002")
        )
      },
      test("an invalid target fails when USED, listing every problem") {
        val broken = Map(
          "dev" -> SdpTarget("localhost:15002", storageRoot = Some("/tmp/sdp"))
        )
        val message = TargetResolution.select(base, broken, "dev", noEnv, "sdpRunOn").swap.getOrElse("")
        assertTrue(
          message.contains("target 'dev' is invalid"),
          message.contains("connectEndpoint"),
          message.contains("storageRoot"),
        )
      },
      test("a target declared but never used is never validated") {
        // P1's practical corollary: a typo'd prod target must not break the
        // offline loop or `sdpRunOn dev`.
        val targets = Map(
          "dev"  -> SdpTarget("sc://localhost:15002"),
          "prod" -> SdpTarget("wss://typo:15002"),
        )
        assertTrue(TargetResolution.select(base, targets, "dev", noEnv, "sdpRunOn").isRight)
      },
      test("no name given asks for one and lists the choices") {
        val targets = Map("dev" -> SdpTarget("sc://localhost:15002"))
        val message = TargetResolution.select(base, targets, "  ", noEnv, "sdpRunOn").swap.getOrElse("")
        assertTrue(message.contains("needs a target name"), message.contains("dev"))
      },
    ),
    suite("the promotion property, as far as a unit test can see it")(
      test("resolution touches nothing the manifest is made of") {
        // Structural, not behavioural: a ResolvedConnection is exactly the
        // connection surface — endpoint, storage, graph defaults, transport,
        // handshake — with no field that could reach graph construction. The
        // scripted suite proves the byte-identical half end to end.
        val dev  = TargetResolution.select(base, Map("dev" -> SdpTarget("sc://localhost:15002")), "dev", noEnv, "sdpRunOn")
        val prodC = TargetResolution.resolve(base, "prod", prod.copy(tokenEnv = None), noEnv)
        assertTrue(
          dev.isRight,
          prodC.isRight,
          dev.map(_.endpoint) != prodC.map(_.endpoint),
          // ResolvedConnection has exactly 7 fields, all connection-shaped
          dev.map(_.productArity) == Right(7),
        )
      }
    ),
  )
