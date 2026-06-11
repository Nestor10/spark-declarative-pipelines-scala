import java.nio.file.Files

val pluginVersion = sys.props.getOrElse("plugin.version", sys.error("plugin.version not set"))

val manifestSnapshot = taskKey[Unit]("Record the current manifest bytes for later comparison")
val checkUnchanged   = taskKey[Unit]("Assert manifest bytes are identical to the snapshot")
val checkNoGold      = taskKey[Unit]("Assert gold_orders is absent")
val checkHasGold     = taskKey[Unit]("Assert gold_orders is present (cache invalidated)")

def manifestPath(t: java.io.File) = t.toPath.resolve("sdp").resolve("pipeline.sdpm")
def snapshotPath(t: java.io.File) = t.toPath.resolve("sdp").resolve("snapshot.bytes")

lazy val root = (project in file("."))
  .enablePlugins(dev.sdp.plugin.SparkPipelinesPlugin)
  .settings(
    name         := "caching",
    scalaVersion := "3.8.4",
    libraryDependencies += "io.github.nestor10" %% "sdp" % pluginVersion,
    sdpPipelineClass := "pipelines.Warehouse",

    manifestSnapshot := {
      val t = (Compile / target).value
      Files.copy(
        manifestPath(t),
        snapshotPath(t),
        java.nio.file.StandardCopyOption.REPLACE_EXISTING,
      )
    },

    checkUnchanged := {
      val t        = (Compile / target).value
      val now      = Files.readAllBytes(manifestPath(t))
      val snapshot = Files.readAllBytes(snapshotPath(t))
      assert(
        java.util.Arrays.equals(now, snapshot),
        "manifest bytes changed across a no-op re-run — caching/determinism is broken",
      )
    },

    checkNoGold := {
      val content = new String(Files.readAllBytes(manifestPath((Compile / target).value)), "UTF-8")
      assert(!content.contains("gold_orders"), s"gold_orders must not exist yet:\n$content")
    },

    checkHasGold := {
      val content = new String(Files.readAllBytes(manifestPath((Compile / target).value)), "UTF-8")
      assert(content.contains("node|gold_orders|streaming-table|delta"), s"gold_orders missing:\n$content")
      assert(content.contains("edge|bronze_orders|gold_orders"), s"gold edge missing:\n$content")
    },
  )
