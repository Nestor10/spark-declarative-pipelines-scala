import java.nio.file.Files

val pluginVersion = sys.props.getOrElse("plugin.version", sys.error("plugin.version not set"))

val checkManifest            = taskKey[Unit]("Assert the generated manifest content")
val checkManifestAfterDelete = taskKey[Unit]("Assert silver is gone after its source was deleted")
val checkSchemas             = taskKey[Unit]("Assert the generated named-tuple schema aliases")
val checkWireDump            = taskKey[Unit]("Assert the prototext wire dump's layout and content")

lazy val root = (project in file("."))
  .enablePlugins(dev.sdp.plugin.SparkPipelinesPlugin)
  .settings(
    name         := "valid-pipeline",
    scalaVersion := "3.9.0",
    // the sdp library is auto-injected by the plugin in lockstep;
    // declaring the DSL explicitly here is harmless and documents the surface.
    libraryDependencies += "io.github.nestor10" %% "sdp" % pluginVersion,
    // The pipeline object the plugin evaluates (classload-eval, D10).
    sdpPipelineClass := "pipelines.Warehouse",

    checkManifest := {
      val path    = (Compile / target).value.toPath.resolve("sdp").resolve("pipeline.sdpm")
      val content = new String(Files.readAllBytes(path), "UTF-8")
      def has(s: String): Unit =
        assert(content.contains(s), s"expected manifest to contain '$s', got:\n$content")
      has("sdp-manifest/2")
      has("node|bronze_orders|table|delta")
      has("node|dim_customers|table|delta")
      has("node|silver_orders|streaming-table|delta")
      // lineage derived from the flow body: stream.table + read.table reads
      has("edge|bronze_orders|silver_orders")
      has("edge|dim_customers|silver_orders")
      // the authored flow itself travels in the manifest (v2)
      has("flow|silver_orders|silver_orders|")
    },

    checkSchemas := {
      val path    = (Compile / scalaSource).value.toPath.resolve("sdp/schemas/PipelineSchemas.scala")
      val content = new String(Files.readAllBytes(path), "UTF-8")
      assert(
        content.contains("type Rates = (value: Long, seen_at: java.sql.Timestamp)"),
        s"expected inferred Rates alias, got:\n$content",
      )
      assert(content.contains("package sdp.schemas"), s"missing package, got:\n$content")
    },

    // The wire dump lands as a real directory of prototext files in a real
    // consumer build — the layout (numbering, one file per command) and the
    // graph-id placeholder are what a reader depends on.
    checkWireDump := {
      val dir = (Compile / target).value.toPath.resolve("sdp").resolve("wire")
      assert(Files.isDirectory(dir), s"expected a wire dump directory at $dir")
      val stream = Files.list(dir)
      val names =
        try {
          val it = stream.iterator()
          val b  = List.newBuilder[String]
          while (it.hasNext) b += it.next().getFileName.toString
          b.result().sorted
        } finally stream.close()

      def has(name: String): Unit =
        assert(names.contains(name), s"expected $name in the wire dump, got: $names")

      has("00-create-dataflow-graph.txtpb")
      has("99-start-run.txtpb")
      assert(
        names.exists(_.endsWith("define-flow-silver_orders.txtpb")),
        s"expected a DefineFlow file for silver_orders, got: $names",
      )
      assert(
        names.exists(_.endsWith("define-output-bronze_orders.txtpb")),
        s"expected a DefineOutput file for bronze_orders, got: $names",
      )
      assert(names.forall(_.endsWith(".txtpb")), s"unexpected files in the dump: $names")

      val flowFile = names.find(_.endsWith("define-flow-silver_orders.txtpb")).get
      val flow     = new String(Files.readAllBytes(dir.resolve(flowFile)), "UTF-8")
      assert(flow.contains("define_flow {"), s"expected prototext DefineFlow, got:\n$flow")
      assert(
        flow.contains("flow_name: \"silver_orders\""),
        s"expected the flow name in the prototext, got:\n$flow",
      )
      assert(
        flow.contains("<dataflow-graph-id>"),
        s"the server-assigned graph id must render as a placeholder, got:\n$flow",
      )
    },

    checkManifestAfterDelete := {
      val path    = (Compile / target).value.toPath.resolve("sdp").resolve("pipeline.sdpm")
      val content = new String(Files.readAllBytes(path), "UTF-8")
      assert(
        !content.contains("silver_orders"),
        s"silver_orders must vanish after deleting its source (no clean!), got:\n$content",
      )
      assert(
        content.contains("node|bronze_orders|table|delta"),
        s"bronze_orders must survive the deletion, got:\n$content",
      )
    },
  )
