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
ThisBuild / scalaVersion := "3.8.4"

// ---------------------------------------------------------------------
// Publishing / POM metadata — required for Maven Central (Sonatype
// Central Portal). Applied to the two published modules via
// `publishSettings` below; the root aggregate sets `publish / skip`.
// ---------------------------------------------------------------------
ThisBuild / versionScheme := Some("early-semver")
ThisBuild / homepage      := Some(url("https://github.com/Nestor10/spark-declarative-pipelines-scala"))
ThisBuild / licenses      := Seq(
  "Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0")
)
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/Nestor10/spark-declarative-pipelines-scala"),
    "scm:git:git@github.com:Nestor10/spark-declarative-pipelines-scala.git",
  )
)
ThisBuild / developers := List(
  Developer(
    id = "Nestor10",
    name = "Eric Smith",
    email = "ericsmith.lpi@gmail.com",
    url = url("https://github.com/Nestor10"),
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

ThisBuild / scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Wunused:all",
  "-Wvalue-discard",
  "-Wnonunit-statement",
)

// Pin ZIO once for every module that needs it.
val zioVersion = "2.1.15"  // TODO: verify against the latest 2.1.x before merging

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
      ("org.apache.spark" % "spark-connect-common_2.13" % "4.1.2").intransitive(),
      // Spark 4.x generates protobuf code against the 4.x runtime
      // (RuntimeVersion checks); ../spark/pom.xml pins 4.33.x.
      "com.google.protobuf" % "protobuf-java" % "4.33.5",
      // gRPC runtime for the SparkConnectServiceGrpc stubs that ship inside
      // spark-connect-common. Version from ../spark/pom.xml (io.grpc.version).
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
    // Forked tests: the container-backed integration specs (AlgebraOracleSpec,
    // FunctionLibrarySpec, PipelinesRegistrationIntegrationSpec) want a clean
    // JVM and the real classpath via java.class.path. The SDP_INTEGRATION gate
    // keeps them off CI without a container engine.
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
  .aggregate(sdp, sbtSparkPipelines)
  .settings(
    name           := "sbt-spark-pipelines-root",
    publish / skip := true,
  )
