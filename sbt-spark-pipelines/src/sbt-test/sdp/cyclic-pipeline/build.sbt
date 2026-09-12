val pluginVersion = sys.props.getOrElse("plugin.version", sys.error("plugin.version not set"))

lazy val root = (project in file("."))
  .enablePlugins(dev.sdp.plugin.SparkPipelinesPlugin)
  .settings(
    name         := "cyclic-pipeline",
    scalaVersion := "3.9.0",
    libraryDependencies += "io.github.nestor10" %% "sdp" % pluginVersion,
    sdpPipelineClass := "pipelines.Warehouse",
  )
