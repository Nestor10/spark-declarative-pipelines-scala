sys.props.get("plugin.version") match {
  case Some(v) => addSbtPlugin("dev.sdp" % "sbt-spark-pipelines" % v)
  case None    => sys.error("The system property 'plugin.version' is not set.")
}
