import java.nio.file.Files
import java.security.MessageDigest

val pluginVersion = sys.props.getOrElse("plugin.version", sys.error("plugin.version not set"))

val manifestSnapshot       = taskKey[Unit]("Record the manifest's SHA-256 for later comparison")
val checkManifestIdentical = taskKey[Unit]("Assert the manifest hash still equals the snapshot")
val checkProdEndpointUsed  = taskKey[Unit]("Assert `sdpDryRunOn prod` dialled PROD's endpoint")

def manifestPath(t: java.io.File) = t.toPath.resolve("sdp").resolve("pipeline.sdpm")
def snapshotPath(t: java.io.File) = t.toPath.resolve("sdp").resolve("manifest.sha256")

def sha256(bytes: Array[Byte]): String =
  MessageDigest.getInstance("SHA-256").digest(bytes).map(b => f"$b%02x").mkString

lazy val root = (project in file("."))
  .enablePlugins(dev.sdp.plugin.SparkPipelinesPlugin)
  .settings(
    name             := "targets",
    scalaVersion     := "3.9.0",
    libraryDependencies += "io.github.nestor10" %% "sdp" % pluginVersion,
    sdpPipelineClass := "pipelines.Warehouse",

    // The flat settings: the implicit "wherever this build points" environment,
    // exactly as every build had before targets existed. `sdpRun`/`sdpDryRun`
    // still use these; the `*On` tasks do not.
    sdpConnectEndpoint := "sc://localhost:1",
    sdpDefaultCatalog  := "warehouse",
    sdpDefaultDatabase := "shared",

    // Declared environments. Every endpoint here is deliberately DEAD: this
    // suite needs no Spark server. Port 1 is never listening and the `.invalid`
    // TLD is reserved by RFC 6761, so both fail fast and deterministically —
    // and a transport failure is exactly the evidence that the target's
    // endpoint was really used.
    sdpTargets := Map(
      // E3: user-scoped by default — the database is dev_<user>, so two
      // engineers sharing a server never collide and neither configures a thing.
      "dev" -> SdpTarget.userScopedDev("sc://localhost:1", catalog = "warehouse"),
      "prod" -> SdpTarget(
        connectEndpoint = "sc://sdp-prod-unreachable.invalid:15002",
        defaultCatalog  = Some("warehouse"),
        defaultDatabase = Some("analytics"),
        storageRoot     = Some("s3a://lake/sdp/prod"),
        deadlineSeconds = Some(10),
      ),
      // A target whose credential comes from an environment variable that is
      // NOT exported here: resolution must fail with a readable message before
      // anything touches the network. (The message text is asserted in
      // TargetResolutionSpec; scripted asserts the failure.)
      "prod-secure" -> SdpTarget(
        connectEndpoint = "sc://sdp-prod-unreachable.invalid:15002",
        useTls          = true,
        tokenEnv        = Some("SDP_SCRIPTED_TOKEN_THAT_IS_NOT_EXPORTED"),
      ),
    ),

    // Def.uncached on every check: sbt 2 would otherwise replay an unchanged
    // task from the action cache, and a skipped assertion asserts nothing.
    manifestSnapshot := Def.uncached {
      val t    = (Compile / target).value
      val hash = sha256(Files.readAllBytes(manifestPath(t)))
      Files.write(snapshotPath(t), hash.getBytes("UTF-8"))
      streams.value.log.info(s"[targets] manifest sha256 snapshot = $hash")
    },

    // THE PROMOTION PROPERTY. The manifest is a pure function of the pipeline
    // sources, never of the environment: registering against a different target
    // — or pointing the whole build at different target values — must leave the
    // bytes untouched. The hash you validated in dev is the hash prod runs.
    checkManifestIdentical := Def.uncached {
      val t        = (Compile / target).value
      val now      = sha256(Files.readAllBytes(manifestPath(t)))
      val snapshot = new String(Files.readAllBytes(snapshotPath(t)), "UTF-8")
      streams.value.log.info(s"[targets] manifest sha256 now = $now / snapshot = $snapshot")
      assert(
        now == snapshot,
        s"PROMOTION PROPERTY VIOLATED: the manifest changed with the environment.\n" +
          s"  snapshot: $snapshot\n  now:      $now",
      )
      streams.value.log.info("[targets] manifest bytes are IDENTICAL across environments")
    },

    // The target's endpoint is really the one dialled: run the input task
    // programmatically, capture the failure, and assert it names prod's
    // endpoint and provenance (the flat setting points at a DIFFERENT dead
    // endpoint, so this cannot pass by accident).
    // Def.uncached: the body consumes a task RESULT (sbt cannot hash a
    // Result[Unit]), and it is a network probe besides.
    checkProdEndpointUsed := Def.uncached {
      val log = streams.value.log
      sdpDryRunOn.toTask(" prod").result.value match {
        case sbt.Result.Inc(incomplete) =>
          val texts =
            sbt.Incomplete.allExceptions(incomplete).toList.flatMap(e => Option(e.getMessage)) ++
              sbt.Incomplete.linearize(incomplete).flatMap(_.message)
          val all = texts.mkString("\n")
          log.info(s"[targets] sdpDryRunOn prod failed with:\n$all")
          assert(
            all.contains("sc://sdp-prod-unreachable.invalid:15002"),
            s"expected the failure to name PROD's endpoint, got:\n$all",
          )
          assert(
            all.contains("target 'prod'"),
            s"expected the failure to name the target it used, got:\n$all",
          )
          assert(
            !all.contains("sc://localhost:1"),
            s"the flat sdpConnectEndpoint leaked into a targeted task:\n$all",
          )
        case sbt.Result.Value(_) =>
          sys.error("sdpDryRunOn prod unexpectedly SUCCEEDED against an unreachable endpoint")
      }
    },
  )
