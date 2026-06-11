val pluginVersion = sys.props.getOrElse("plugin.version", sys.error("plugin.version not set"))

lazy val root = (project in file("."))
  .enablePlugins(dev.sdp.plugin.SparkPipelinesPlugin)
  .settings(
    name         := "cyclic-pipeline",
    scalaVersion := "3.8.4",
    libraryDependencies += "dev.sdp" %% "sdp-runtime-dsl" % pluginVersion,
    sdpPipelineClass := "pipelines.Warehouse",
  )
