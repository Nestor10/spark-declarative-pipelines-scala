package dev.sdp.plugin

import zio.test.*

/** The environment descriptor: validation and the user-scoped dev default (E1 /
  * E3).
  *
  * These are the plugin module's first unit tests, and they are unit tests on
  * purpose: a target's rules are pure functions of the values in `build.sbt`, so
  * they do not need sbt, a server, or a scripted sandbox. The scripted suite
  * (`src/sbt-test/sdp/targets`) covers what only a real build can show — that
  * the manifest is target-independent and that an `*On` task actually dials the
  * target's endpoint.
  */
object SdpTargetSpec extends ZIOSpecDefault:

  def spec = suite("SdpTarget")(
    suite("validation (applied when a target is USED, not at build load)")(
      test("a minimal target with a well-formed endpoint is valid") {
        assertTrue(SdpTarget("sc://localhost:15002").problems.isEmpty)
      },
      test("a fully specified target is valid") {
        val target = SdpTarget(
          connectEndpoint = "sc://spark-connect.prod.svc.cluster.local:15002",
          defaultCatalog = Some("warehouse"),
          defaultDatabase = Some("analytics"),
          storageRoot = Some("s3a://lake/sdp/prod"),
          useTls = true,
          tokenEnv = Some("SDP_PROD_TOKEN"),
          deadlineSeconds = Some(120),
          versionCheck = Some(false),
          schedule = Some("0 * * * *"),
          namespace = Some("data-prod"),
          runnerImage = Some("ghcr.io/acme/sdp-runner:1.2.3"),
        )
        assertTrue(target.problems.isEmpty)
      },
      test("the endpoint must be sc://host:port") {
        val bad = List(
          "localhost:15002",                // no scheme
          "http://localhost:15002",         // wrong scheme
          "sc://localhost",                 // no port
          "sc://localhost:15002/pipelines", // path is not part of the contract
          "sc://:15002",                    // no host
          "",
        )
        assertTrue(
          bad.forall(e => SdpTarget(e).problems.nonEmpty),
          // the message quotes the value back, so the fix is obvious
          bad.forall(e => SdpTarget(e).problems.exists(_.contains("connectEndpoint"))),
        )
      },
      test("the endpoint port must be a number in range, and the message says so") {
        val problems = SdpTarget("sc://localhost:not-a-port").problems
        assertTrue(
          problems.size == 1,
          problems.head.contains("1-65535"),
          problems.head.contains("not-a-port"),
          SdpTarget("sc://localhost:0").problems.nonEmpty,
          SdpTarget("sc://localhost:70000").problems.nonEmpty,
        )
      },
      test("a storage root must be an absolute URI WITH a scheme") {
        val relative = SdpTarget("sc://h:1", storageRoot = Some("/tmp/sdp")).problems
        assertTrue(
          relative.size == 1,
          relative.head.contains("absolute URI with a scheme"),
          SdpTarget("sc://h:1", storageRoot = Some("file:///tmp/sdp")).problems.isEmpty,
          SdpTarget("sc://h:1", storageRoot = Some("s3a://lake/sdp")).problems.isEmpty,
        )
      },
      test("a blank Some(...) is a mistake, not an omission") {
        assertTrue(
          SdpTarget("sc://h:1", defaultCatalog = Some("  ")).problems.exists(_.contains("blank")),
          SdpTarget("sc://h:1", defaultDatabase = Some("")).problems.exists(_.contains("blank")),
          SdpTarget("sc://h:1", storageRoot = Some(" ")).problems.nonEmpty,
          SdpTarget("sc://h:1", tokenEnv = Some("")).problems.exists(_.contains("blank")),
        )
      },
      test("tokenEnv must look like a variable NAME, not a token") {
        // The shape of the field is the real defence (there is no `token:
        // String` to put a secret in); this catches the obvious slip.
        assertTrue(
          SdpTarget("sc://h:1", tokenEnv = Some("dapi abc123")).problems
            .exists(_.contains("NAME of an environment variable"))
        )
      },
      test("a non-positive deadline is refused") {
        assertTrue(
          SdpTarget("sc://h:1", deadlineSeconds = Some(0)).problems.nonEmpty,
          SdpTarget("sc://h:1", deadlineSeconds = Some(-5)).problems.nonEmpty,
          SdpTarget("sc://h:1", deadlineSeconds = Some(1)).problems.isEmpty,
        )
      },
      test("every problem is reported at once, not just the first") {
        val target = SdpTarget(
          connectEndpoint = "nope",
          storageRoot = Some("/relative"),
          tokenEnv = Some(""),
        )
        assertTrue(target.problems.size == 3)
      },
    ),
    suite("userScopedDev (E3 — collision-free by default)")(
      test("the database is dev_<sanitized user.name>") {
        val target = SdpTarget.userScopedDev("sc://localhost:15002", catalog = "warehouse")
        val expected = SdpTarget.devDatabaseFor(sys.props.getOrElse("user.name", ""))
        assertTrue(
          target.connectEndpoint == "sc://localhost:15002",
          target.defaultCatalog == Some("warehouse"),
          target.defaultDatabase == Some(expected),
          expected.startsWith("dev_"),
          target.problems.isEmpty,
        )
      },
      test("the catalog is optional — an empty string omits it") {
        assertTrue(
          SdpTarget.userScopedDev("sc://localhost:15002").defaultCatalog.isEmpty,
          SdpTarget.userScopedDev("sc://localhost:15002", catalog = "  ").defaultCatalog.isEmpty,
        )
      },
      test("a dev target is plaintext + anonymous, like the local container") {
        val target = SdpTarget.userScopedDev("sc://localhost:15002")
        assertTrue(
          !target.useTls,
          target.tokenEnv.isEmpty,
          target.storageRoot.isEmpty, // inherits sdpStorageRoot
        )
      },
      test("sanitization: lower-cased, non-alphanumerics become underscores") {
        assertTrue(
          SdpTarget.devDatabaseFor("eric") == "dev_eric",
          SdpTarget.devDatabaseFor("Eric") == "dev_eric",
          SdpTarget.devDatabaseFor("Eric Smith") == "dev_eric_smith",
          SdpTarget.devDatabaseFor("eric.smith") == "dev_eric_smith",
          SdpTarget.devDatabaseFor("ACME\\eric") == "dev_acme_eric",
          SdpTarget.devDatabaseFor("eric-smith+1") == "dev_eric_smith_1",
          SdpTarget.devDatabaseFor("ericsmith.lpi@gmail.com") == "dev_ericsmith_lpi_gmail_com",
        )
      },
      test("edge users degrade to a legal identifier instead of a broken one") {
        assertTrue(
          SdpTarget.devDatabaseFor("") == "dev_user",
          SdpTarget.devDatabaseFor("   ") == "dev_user",
          SdpTarget.devDatabaseFor("!!!") == "dev_user",
          // leading/trailing separators are trimmed, not left dangling
          SdpTarget.devDatabaseFor("_eric_") == "dev_eric",
          SdpTarget.devDatabaseFor("7eric") == "dev_7eric",
        )
      },
      test("the derived database is itself a valid target value") {
        val users = List("eric", "Eric Smith", "", "ACME\\eric", "ünïcode")
        assertTrue(
          users.forall { u =>
            val db = SdpTarget.devDatabaseFor(u)
            db.startsWith("dev_") && db.forall(c => c.isLetterOrDigit || c == '_')
          }
        )
      },
    ),
  )
