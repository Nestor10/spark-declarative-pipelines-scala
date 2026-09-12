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
            .mapError {
              case e: StatusRuntimeException =>
                RegistrationError.ServerRejected(s"${e.getStatus.getCode}: ${e.getStatus.getDescription}")
              case other => RegistrationError.TransportFailure(other.toString)
            }
        }
    }
