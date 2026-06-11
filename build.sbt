// =====================================================================
// sbt-spark-pipelines — multi-module build
//
// Three modules form the Onion (dependencies flow inward only):
//
//   sbt-spark-pipelines  (Infrastructure: sbt AutoPlugin + Spark Connect)
//          │
//          ▼ depends on
//   sdp-core             (Domain Core + Application Services / ZIO)
//          ▲
//          │ depends on
//   sdp-runtime-dsl      (Infrastructure: macro DSL for authors)
//
// `sdp-runtime-dsl` and `sbt-spark-pipelines` both depend on `sdp-core`
// but NEVER on each other — they're sibling Infrastructure adapters.
// =====================================================================

ThisBuild / organization := "dev.sdp"           // TODO: replace with real org coords before first publish
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.8.4"

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
// sdp-core — pure Domain + ZIO Application Services
// ---------------------------------------------------------------------
lazy val sdpCore = (project in file("sdp-core"))
  .settings(
    name := "sdp-core",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio"          % zioVersion,
      "dev.zio" %% "zio-test"     % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test,
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
  )

// ---------------------------------------------------------------------
// sdp-runtime-dsl — Scala 3 macro DSL used by pipeline authors
//   Uses only stdlib `scala.quoted.*` / `quotes.reflect.*` — no extra
//   compiler dependency needed.
// ---------------------------------------------------------------------
lazy val sdpRuntimeDsl = (project in file("sdp-runtime-dsl"))
  .dependsOn(sdpCore)
  .settings(
    name := "sdp-runtime-dsl",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio-test"     % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test,
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
  )

// ---------------------------------------------------------------------
// sbt-spark-pipelines — the sbt 2.0 AutoPlugin
//   Drives macro extraction at compile time, validates via sdp-core,
//   and pushes the resulting Protobuf graph to Spark Connect.
// ---------------------------------------------------------------------
lazy val sbtSparkPipelines = (project in file("sbt-spark-pipelines"))
  .dependsOn(sdpCore, sdpRuntimeDsl % "test->compile")
  .enablePlugins(SbtPlugin)
  .settings(
    name := "sbt-spark-pipelines",
    libraryDependencies ++= Seq(
      // Reads macro-embedded fragment constants out of .tasty files —
      // no classloading of user code, ever. See ROADMAP "Fragment discovery".
      "ch.epfl.scala" %% "tasty-query"  % "1.8.0",
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
      "dev.zio"       %% "zio-streams"  % zioVersion,
      "dev.zio"       %% "zio-test"     % zioVersion % Test,
      "dev.zio"       %% "zio-test-sbt" % zioVersion % Test,
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
    // Forked tests see the real classpath via java.class.path — the TASTy
    // scanner spec feeds it to tasty-query.
    Test / fork := true,

    // sbt 2.0 publishes plugins in standard Maven layout (artifact suffix
    // `_sbt2_3`). Disable the legacy ivy-style layout so publishM2 / Central
    // Portal accept the POM. (Verified: this key exists in the RC15 jars.)
    sbtPluginPublishLegacyMavenStyle := false,

    // Version lockstep: bake the plugin's own version into a constant so it
    // can inject the matching `sdp-runtime-dsl` into consumer builds. A
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
           |  * release version so it can inject the matching runtime DSL. */
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
    //
    // libraryDependencies += ("org.apache.spark" %% "spark-connect-client-jvm" % sparkVersion)
    //   .cross(CrossVersion.for3Use2_13),

    // Scripted integration tests live under src/sbt-test.
    scriptedLaunchOpts ++= Seq(
      "-Xmx1024m",
      "-Dplugin.version=" + version.value,
    ),
    scriptedBufferLog := false,
    // Sandbox builds resolve the plugin AND the runtime libraries from the
    // local ivy repo — publish everything before running scripted.
    scriptedDependencies := {
      val a = (sdpCore / publishLocal).value
      val b = (sdpRuntimeDsl / publishLocal).value
      val c = publishLocal.value
    },
  )

// ---------------------------------------------------------------------
// Root aggregate — coordination only, never published.
// ---------------------------------------------------------------------
lazy val root = (project in file("."))
  .aggregate(sdpCore, sdpRuntimeDsl, sbtSparkPipelines)
  .settings(
    name           := "sbt-spark-pipelines-root",
    publish / skip := true,
  )
