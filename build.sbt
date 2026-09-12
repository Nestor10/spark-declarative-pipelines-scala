// =====================================================================
// sbt-spark-pipelines — multi-module build
//
// TWO published artifacts (collapsed from four, 2026-06-11 — Maven
// artifacts are a DISTRIBUTION boundary, not an architecture boundary;
// the three library rings always shipped in lockstep, so they ship as
// one artifact and the onion lives in the package structure):
//
//   sbt-spark-pipelines  (Infrastructure: thin sbt AutoPlugin wrapper)
//          │
//          ▼ depends on
//   sdp                  (everything else, one artifact:
//          dev.sdp.core     — Domain Core + codecs (zero deps)
//          dev.sdp.app      — ZIO Application Services (assembly/validation)
//          dev.sdp.dsl      — runtime plan-builder DSL (author surface)
//          dev.sdp.connect  — Spark Connect gRPC client + SdpApp runner)
//
// Onion rule (now by convention + review, not module boundaries):
// dependencies flow inward only — core ← app ← {dsl, connect}; dsl and
// connect never depend on each other in main (connect's tests may author
// pipelines through the dsl). The plugin gets everything via `sdp`; the
// library-first SdpApp runner ships in the user's uber jar, scheduled by
// Argo/K8s, with no sbt on the path (D10).
// =====================================================================

// Maven coordinates. Central's GitHub-verified namespace for github.com/Nestor10.
// NOTE: this is the Maven groupId ONLY — the Scala package names stay `dev.sdp.*`.
// The plugin bakes this value into `SdpBuildInfo.organization` (sourceGenerator
// below) so the version-lockstep injection emits matching coordinates.
ThisBuild / organization := "io.github.nestor10"
// Version is OWNED BY sbt-dynver (via sbt-ci-release): a `vX.Y.Z` git tag
// publishes X.Y.Z; between tags you get X.Y.Z+N-<hash>-SNAPSHOT. Do not set
// `ThisBuild / version` — see RELEASING.md.
// CEILING: sbt 2.0.8 loads plugins (and their deps) with its own Scala 3.8.x —
// 3.9.0-compiled TASTy (28.9) is unreadable in every consumer metabuild, so the
// plugin AND sdp must stay on the newest Scala whose TASTy sbt can read. Bump
// only when sbt's shipped Scala bumps. (Proven 2026-09-12: 3.9.0 → scripted
// fails with "Forward incompatible TASTy 28.9"; 3.8.4 → green. User projects
// on 3.9+ still consume the 3.8.4-built sdp fine — forward-compatible TASTy;
// the scripted sandboxes pin 3.9.0 to keep proving exactly that.)
ThisBuild / scalaVersion := "3.8.4"

// ---------------------------------------------------------------------
// Publishing / POM metadata — required for Maven Central (Sonatype
// Central Portal). Applied to the two published modules via
// `publishSettings` below; the root aggregate sets `publish / skip`.
// ---------------------------------------------------------------------
ThisBuild / versionScheme := Some("early-semver")
ThisBuild / homepage      := Some(uri("https://github.com/Nestor10/spark-declarative-pipelines-scala"))
ThisBuild / licenses      := Seq(
  "Apache-2.0" -> uri("https://www.apache.org/licenses/LICENSE-2.0")
)
ThisBuild / scmInfo := Some(
  ScmInfo(
    uri("https://github.com/Nestor10/spark-declarative-pipelines-scala"),
    "scm:git:git@github.com:Nestor10/spark-declarative-pipelines-scala.git",
  )
)
ThisBuild / developers := List(
  Developer(
    id = "Nestor10",
    name = "Eric Smith",
    email = "ericsmith.lpi@gmail.com",
    url = uri("https://github.com/Nestor10"),
  )
)

// Shared publish settings for the published modules. Central requires a
// standard Maven POM plus sources + javadoc jars (sbt produces both by
// default — we just don't disable them). Sonatype Central Portal hosts
// the staging repo; the actual upload is driven from RELEASING.md.
// publishTo is managed by sbt-ci-release (sonatypePublishToBundle); we only
// keep Maven style explicit. POM metadata above is what Central validates.
lazy val publishSettings = Seq(
  publishMavenStyle := true,
)

// Sonatype Central's server-side processing is the release bottleneck and it is
// NOT ours: measured 2026-09-12, the whole sbt side (compile+sign+bundle+upload)
// takes ~47 s, then the plugin polls Central until PUBLISHED — 49 min (v0.2.0)
// and 57.5 min (v0.2.1), the latter 2.5 min under sbt-sonatype's default 60-min
// timeout, whose expiry marks the run RED while Sonatype publishes anyway (and
// re-running the job on that false failure would double-deploy). Nothing
// client-side makes the artifact appear sooner, so: wait generously, never
// serialize work on the green check — consumers should poll repo1 for the
// version instead (that is exactly what the example-refresh flow does).
ThisBuild / sonatypeTimeoutMillis := 3 * 60 * 60 * 1000 // 3h, was 60min default

// Explicit over inherited: sbt-ci-release 1.11.x already defaults to the
// Sonatype Central Portal (proven — both 0.2.x deployments landed there),
// but pin it so a future plugin-default change can't silently reroute us.
// The legacy hosts (oss.sonatype.org / s01) are retired.
ThisBuild / sonatypeCredentialHost := xerial.sbt.Sonatype.sonatypeCentralHost

ThisBuild / scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Wunused:all",
  "-Wvalue-discard",
  "-Wnonunit-statement",
)

// Pin ZIO once for every module that needs it.
val zioVersion = "2.1.26"

// The pinned Spark Connect wire version — the ONE place it is written.
//
// Overridable at sbt LAUNCH time — as `SBT_OPTS=-Dsdp.connect.common.version=…`,
// because sbt 2's thin client does NOT forward a command-line -D to the server
// JVM, and a warm server has already evaluated this file (the script stops it
// first and verifies the resolved version) — for exactly one purpose: the
// upstream-watch workflow
// (`scripts/upstream-watch.sh`) re-renders the conformance inventory against a
// candidate release and diffs it against the committed snapshot, WITHOUT
// editing build.sbt. Normal builds never pass the property and are byte-for-byte
// unchanged. The pin itself is only ever bumped by hand — protobuf-java and
// grpc must match the chosen Spark release's own pom (see DECISIONS D2), which
// is a human judgment, so the watch reports and never commits.
//
// This is a setting evaluated at build LOAD, not inside a cached task body —
// the CLAUDE.md prohibition is on `sys.props` in cached tasks, where it would
// break cache stability.
val sparkConnectCommonVersion = sys.props.getOrElse("sdp.connect.common.version", "4.2.0")

// ---------------------------------------------------------------------
// sdp — THE library: pure domain core + ZIO app services + runtime
//   plan-builder DSL + Spark Connect client + the SdpApp runner.
//   One artifact because every consumer needs all of it, always, in
//   lockstep (collapsing killed the CoreEpoch cross-artifact hazard,
//   the 3-way lockstep injection, and most of the local-publish dance).
// ---------------------------------------------------------------------
lazy val sdp = (project in file("sdp"))
  .settings(publishSettings)
  .settings(
    name := "sdp",
    libraryDependencies ++= Seq(
      // Spark's own generated protobuf classes (pure Java) — the wire
      // contract for the SDP PipelinesHandler, identical to the server's by
      // construction. intransitive: keep Spark's 2.13 closure out of our
      // graph; protobuf-java is the one real runtime need.
      // 4.2.0: adds AutoCdcFlowDetails (SCD1/SCD2) + once flows to the wire —
      // see PipelineProtoEncoder GATE(spark-4.2). Version from
      // `sparkConnectCommonVersion` above (launch-overridable for the
      // upstream-watch job only).
      ("org.apache.spark" % "spark-connect-common_2.13" % sparkConnectCommonVersion).intransitive(),
      // Spark 4.x generates protobuf code against the 4.x runtime
      // (RuntimeVersion checks); Spark v4.2.0 pom pins 4.33.5 — match it,
      // don't chase latest.
      "com.google.protobuf" % "protobuf-java" % "4.33.5",
      // gRPC runtime for the SparkConnectServiceGrpc stubs that ship inside
      // spark-connect-common. Spark v4.2.0 pom pins io.grpc.version 1.76.0.
      "io.grpc" % "grpc-netty-shaded" % "1.76.0",
      "io.grpc" % "grpc-stub"         % "1.76.0",
      "io.grpc" % "grpc-protobuf"     % "1.76.0",
      // ZStream models the run's gRPC server-stream of progress events
      // (Zionomicon ch.36: server streaming = ZStream); core ZIO module.
      "dev.zio"       %% "zio"          % zioVersion,
      "dev.zio"       %% "zio-streams"  % zioVersion,
      "dev.zio"       %% "zio-test"     % zioVersion % Test,
      "dev.zio"       %% "zio-test-sbt" % zioVersion % Test,
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
    // Forked tests: a clean JVM and the real classpath via java.class.path.
    // (The container-backed suites moved to `sdpIt` — S1.5, 2026-09-12 — so
    // this module's suite is structurally offline.)
    Test / fork := true,
  )

// ---------------------------------------------------------------------
// sdp-it — the integration / e2e suite. TEST-ONLY, never published.
//
//   Everything here needs a live Spark Connect server (started as a
//   container through the podman/docker CLI — D3 unchanged: still no
//   Testcontainers), so it is slow and environment-dependent, and it stays
//   out of `sdp/testFull`. sbt itself deprecated the `IntegrationTest`
//   configuration in favour of exactly this shape: a separate subproject.
//
//   `test->test` on `sdp` so the offline fixtures/helpers stay where the
//   offline suites also use them — nothing is duplicated.
//
//   D11 is not violated: D11's boundary is DISTRIBUTION, and this module
//   publishes nothing.
// ---------------------------------------------------------------------
lazy val sdpIt = (project in file("sdp-it"))
  .dependsOn(sdp % "compile->compile;test->test")
  .settings(
    name           := "sdp-it",
    publish / skip := true,
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio-test"     % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test,
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
    // Same reason as `sdp`: the container-backed specs want a clean JVM and
    // the real classpath via java.class.path. The SDP_INTEGRATION env gate
    // (unchanged, inside each spec) keeps them skipping where no container
    // engine exists — compilation always succeeds.
    Test / fork := true,
  )

// ---------------------------------------------------------------------
// sbt-spark-pipelines — the sbt 2.0 AutoPlugin
//   A thin convenience wrapper: evaluates the user's pipeline object in an
//   isolated child classloader (classload-eval, D10), validates via the
//   sdp library, and pushes the resulting Protobuf graph to Spark Connect.
//   The library-first SdpApp runner does the same off sbt.
// ---------------------------------------------------------------------
lazy val sbtSparkPipelines = (project in file("sbt-spark-pipelines"))
  .dependsOn(sdp)
  .enablePlugins(SbtPlugin)
  .settings(publishSettings)
  .settings(
    name := "sbt-spark-pipelines",
    libraryDependencies ++= Seq(
      // Fragment discovery is classload-eval (D10), not TASTy scanning: the
      // plugin loads the user's pipeline object in an isolated child
      // classloader, calls `pipeline`, and gets the fragments back as STRINGS
      // via dev.sdp.core.PipelineExport. The fragment string is the ONLY thing
      // that crosses the loader boundary — the exact contract the old TASTy
      // embedding used — so no compiler/TASTy reader (tasty-query) is needed.
      // Everything else arrives via `.dependsOn(sdp)` above.
      "dev.zio"       %% "zio-test"     % zioVersion % Test,
      "dev.zio"       %% "zio-test-sbt" % zioVersion % Test,
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),

    // sbt 2.0 publishes plugins in standard Maven layout (artifact suffix
    // `_sbt2_3`). Disable the legacy ivy-style layout so publishM2 / Central
    // Portal accept the POM. (Verified: this key exists in the RC15/RC16 jars.)
    sbtPluginPublishLegacyMavenStyle := false,

    // Version lockstep: bake the plugin's own version into a constant so it
    // can inject the matching `sdp` library into consumer builds. A
    // sourceGenerator (zero new deps) instead of sbt-buildinfo — keeps the
    // dependency budget tight and avoids relying on an unverified
    // sbt-buildinfo `_sbt2_3` artifact.
    Compile / sourceGenerators += Def.task {
      val file = (Compile / sourceManaged).value / "dev" / "sdp" / "plugin" / "SdpBuildInfo.scala"
      IO.write(
        file,
        s"""package dev.sdp.plugin
           |
           |/** Generated at build time — do not edit. Carries the plugin's own
           |  * release version so it can inject the matching sdp library. */
           |private[plugin] object SdpBuildInfo:
           |  final val version: String      = "${version.value}"
           |  final val organization: String = "${organization.value}"
           |""".stripMargin,
      )
      Seq(file)
    }.taskValue,
    // TODO: add the JVM Spark Connect client once we pin a Spark version.
    // Spark publishes `spark-connect-client-jvm` only for Scala 2.13; we'll
    // consume it from Scala 3 via CrossVersion.for3Use2_13 when wired up.

    // Scripted integration tests live under src/sbt-test.
    scriptedLaunchOpts ++= Seq(
      "-Xmx1024m",
      "-Dplugin.version=" + version.value,
    ),
    scriptedBufferLog := false,
    // Sandbox builds resolve the plugin AND the sdp library from the
    // local ivy repo — publish both before running scripted.
    scriptedDependencies := {
      val a = (sdp / publishLocal).value
      val b = publishLocal.value
    },
  )

// ---------------------------------------------------------------------
// Root aggregate — coordination only, never published.
// ---------------------------------------------------------------------
lazy val root = (project in file("."))
  .aggregate(sdp, sdpIt, sbtSparkPipelines)
  .settings(
    name           := "sbt-spark-pipelines-root",
    publish / skip := true,
  )
