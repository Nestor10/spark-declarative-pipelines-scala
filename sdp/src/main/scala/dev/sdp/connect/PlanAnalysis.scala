package dev.sdp.connect

import java.util.UUID

import scala.jdk.CollectionConverters.*

import io.grpc.StatusRuntimeException
import org.apache.spark.connect.proto as sc
import zio.*

import PipelinesRegistration.RegistrationError

/** The semantic oracle: ask the server's Catalyst analyzer for the schema of
  * a relation. If `AnalyzePlan` resolves a plan we built, the plan is real
  * Spark — not our opinion of Spark. Every algebra capability claim is
  * verified through this call before it enters `SupportedCapabilities`.
  */
object PlanAnalysis:

  /** A field of the analyzed schema: name and protobuf type-kind. */
  final case class SchemaField(name: String, kind: String)

  /** Ask the server what Spark it is, on a channel the caller already owns.
    *
    * `AnalyzePlanRequest.SparkVersion` is the cheapest round trip Spark Connect
    * has — no plan, no analysis, just a string (the recipe proven by
    * `DatabricksProbe`, promoted to production for the registration handshake).
    * It must run on the SAME channel and session as the registration that
    * follows, or the answer describes a different server than the one we are
    * about to push a graph to.
    *
    * Failure stays in the typed channel: a probe that cannot reach the server
    * is information, not a defect. [[PipelinesRegistration.register]] treats a
    * failed probe as "unknown version" and proceeds — the very next RPC will
    * report the real transport problem with far better words than a handshake
    * can.
    */
  private[connect] def sparkVersionOn(
      stub: sc.SparkConnectServiceGrpc.SparkConnectServiceBlockingStub,
      sessionId: String,
      transport: TransportConfig,
  ): IO[RegistrationError, String] =
    ZIO
      .attemptBlockingInterrupt {
        val request = sc.AnalyzePlanRequest
          .newBuilder()
          .setSessionId(sessionId)
          .setUserContext(sc.UserContext.newBuilder().setUserId("sbt-spark-pipelines"))
          .setSparkVersion(sc.AnalyzePlanRequest.SparkVersion.newBuilder())
          .build()
        ConnectChannel.withDeadline(stub, transport).analyzePlan(request).getSparkVersion.getVersion
      }
      .mapError(analysisError)

  def analyzeSchema(
      host: String,
      port: Int,
      relation: sc.Relation,
      // Transport security + per-RPC deadline; plaintext/anonymous by default.
      transport: TransportConfig = TransportConfig.plaintext,
  ): IO[RegistrationError, List[SchemaField]] =
    ZIO.scoped {
      ConnectChannel
        .scoped(host, port, transport)
        .flatMap { channel =>
          ZIO
            .attemptBlockingInterrupt {
              val stub = ConnectChannel
                .withDeadline(sc.SparkConnectServiceGrpc.newBlockingStub(channel), transport)
              val request = sc.AnalyzePlanRequest
                .newBuilder()
                .setSessionId(UUID.randomUUID().toString)
                .setUserContext(sc.UserContext.newBuilder().setUserId("sbt-spark-pipelines"))
                .setSchema(
                  sc.AnalyzePlanRequest.Schema
                    .newBuilder()
                    .setPlan(sc.Plan.newBuilder().setRoot(relation))
                )
                .build()

              val schema = stub.analyzePlan(request).getSchema.getSchema
              schema.getStruct.getFieldsList.asScala.toList.map { field =>
                SchemaField(field.getName, field.getDataType.getKindCase.name.toLowerCase)
              }
            }
            .mapError(analysisError)
        }
    }

  /** Shared failure mapping for every `AnalyzePlan` call here: a gRPC status is
    * the server's verdict, anything else is transport. */
  private def analysisError(cause: Throwable): RegistrationError = cause match
    case e: StatusRuntimeException =>
      RegistrationError.ServerRejected(s"${e.getStatus.getCode}: ${e.getStatus.getDescription}")
    case other => RegistrationError.TransportFailure(other.toString)
