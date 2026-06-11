sys.props.get("plugin.version") match {
  case Some(v) => addSbtPlugin("io.github.nestor10" % "sbt-spark-pipelines" % v)
  case None    => sys.error("The system property 'plugin.version' is not set.")
}
