package dev.sdp.plugin.connect

import java.nio.file.{Files, Paths}
import java.util.UUID

import scala.jdk.CollectionConverters.*

import dev.sdp.plugin.PipelineProtoEncoder
import io.grpc.{ManagedChannelBuilder, Metadata}
import io.grpc.stub.MetadataUtils
import org.apache.spark.connect.proto as sc

/** One-off feasibility probe against a Databricks **serverless** Spark Connect
  * endpoint. NOT a CI test — run manually:
  *
  * {{{
  * sbt 'sbtSparkPipelines/Test/runMain dev.sdp.plugin.connect.DatabricksProbe \
  *   dbc-XXXX.cloud.databricks.com secrets/pat.txt'
  * }}}
  *
  * Answers the only question that decides Path A: does Databricks' Spark
  * Connect server carry the OSS SDP gRPC surface? It (1) confirms basic Spark
  * Connect works over TLS+PAT, then (2) fires a real `CreateDataflowGraph`
  * `PipelineCommand` and prints exactly what comes back — accepted (graph id)
  * or rejected (and how).
  */
object DatabricksProbe:

  def main(args: Array[String]): Unit =
    val host  = args(0)
    val token = Files.readString(Paths.get(args(1))).trim

    // arg(2): a classic cluster id, or "auto" (default) for serverless.
    val compute   = if args.length > 2 then args(2) else "auto"
    // Serverless routing (from databricks-connect 16.x session.py): serverless mode
    // sends NO cluster-id — it generates a client-side UUID and passes it as BOTH
    // the `x-databricks-session-id` header AND the Spark Connect session_id. The
    // server provisions an on-demand serverless session keyed by that id.
    val sessionId = UUID.randomUUID().toString

    val md = new Metadata()
    md.put(Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER), s"Bearer $token")
    if compute == "auto" then
      md.put(Metadata.Key.of("x-databricks-session-id", Metadata.ASCII_STRING_MARSHALLER), sessionId)
    else
      md.put(Metadata.Key.of("x-databricks-cluster-id", Metadata.ASCII_STRING_MARSHALLER), compute)

    // No usePlaintext() → TLS (the default); attach the PAT on every call.
    val channel = ManagedChannelBuilder
      .forAddress(host, 443)
      .intercept(MetadataUtils.newAttachHeadersInterceptor(md))
      .build()
    val stub = sc.SparkConnectServiceGrpc.newBlockingStub(channel)
    def ctx  = sc.UserContext.newBuilder().setUserId("sbt-spark-pipelines").build()

    println(s"=== probing sc://$host:443 (serverless, TLS, bearer PAT) ===")

    // [0] what base Spark version is this serverless compute? (decides whether a
    // missing SDP surface is a version gap or a deliberate Databricks omission)
    try
      val vreq = sc.AnalyzePlanRequest
        .newBuilder()
        .setSessionId(sessionId)
        .setUserContext(ctx)
        .setSparkVersion(sc.AnalyzePlanRequest.SparkVersion.newBuilder())
        .build()
      println(s"[0] serverless Spark version: ${stub.analyzePlan(vreq).getSparkVersion.getVersion}")
    catch case e: Throwable => println(s"[0] Spark version query FAILED: ${e.getMessage}")

    // [1] basic Spark Connect: analyze range(0,5) — proves transport + a live session
    try
      val plan = sc.Plan
        .newBuilder()
        .setRoot(sc.Relation.newBuilder().setRange(sc.Range.newBuilder().setStart(0).setEnd(5).setStep(1)))
      val req = sc.AnalyzePlanRequest
        .newBuilder()
        .setSessionId(sessionId)
        .setUserContext(ctx)
        .setSchema(sc.AnalyzePlanRequest.Schema.newBuilder().setPlan(plan))
        .build()
      val schema = stub.analyzePlan(req).getSchema.getSchema
      println(s"[1] basic Spark Connect OK — range schema: ${schema.getStruct.getFieldsList.asScala.map(_.getName)}")
    catch case e: Throwable => println(s"[1] basic Spark Connect FAILED: $e")

    // [2] the real question: does Databricks accept our SDP PipelineCommand?
    try
      val cmd = PipelineProtoEncoder.createDataflowGraph()
      val req = sc.ExecutePlanRequest
        .newBuilder()
        .setSessionId(sessionId)
        .setUserContext(ctx)
        .setPlan(sc.Plan.newBuilder().setCommand(sc.Command.newBuilder().setPipelineCommand(cmd)))
        .build()
      val it      = stub.executePlan(req)
      val results = scala.collection.mutable.ListBuffer.empty[String]
      while it.hasNext do
        val r = it.next()
        if r.hasPipelineCommandResult then results += r.getPipelineCommandResult.toString.trim
      println(s"[2] CreateDataflowGraph ACCEPTED ✅ — ${if results.isEmpty then "(no result payload)" else results.mkString}")
    catch case e: Throwable => println(s"[2] CreateDataflowGraph REJECTED — ${e.getMessage}")

    channel.shutdownNow()
