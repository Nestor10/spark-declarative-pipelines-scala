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

  /** Which `AnalyzePlanRequest.Explain.ExplainMode` to ask for.
    *
    * Two of the proto's five, on purpose. `EXTENDED` is the one that carries
    * the **Parsed Logical Plan** section — the planner's raw decode of our
    * bytes, before catalog resolution — which is the only section that answers
    * "how does the server read what we sent?" (see [[PlanDiagnostics]]).
    * `FORMATTED` is the readable physical plan, for when the question is what
    * the query will actually *do*. `SIMPLE` is a subset of both, and
    * `CODEGEN`/`COST` answer performance questions this tool does not ask; a
    * mode nobody here has a use for is a surface we would have to keep.
    */
  enum ExplainMode(private[connect] val proto: sc.AnalyzePlanRequest.Explain.ExplainMode):
    case Extended
        extends ExplainMode(sc.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_EXTENDED)
    case Formatted
        extends ExplainMode(sc.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_FORMATTED)

    /** The mode's name as a user reads it in a header line. */
    def label: String = this match
      case Extended  => "extended"
      case Formatted => "formatted"

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
      transport: ConnectTransport,
      sessionId: String,
  ): IO[RegistrationError, String] =
    transport
      .analyze(
        sc.AnalyzePlanRequest
          .newBuilder()
          .setSessionId(sessionId)
          .setUserContext(sc.UserContext.newBuilder().setUserId("sbt-spark-pipelines"))
          .setSparkVersion(sc.AnalyzePlanRequest.SparkVersion.newBuilder())
          .build()
      )
      .map(_.getSparkVersion.getVersion)
      .mapError(analysisError)

  /** Ask the analyzer for a relation's schema on a transport the caller owns —
    * the seam-facing form, drivable by a stub transport offline. */
  private[connect] def analyzeSchemaOn(
      relation: sc.Relation
  ): ZIO[ConnectTransport, RegistrationError, List[SchemaField]] =
    ZIO.serviceWithZIO[ConnectTransport] { transport =>
      transport
        .analyze(
          sc.AnalyzePlanRequest
            .newBuilder()
            .setSessionId(UUID.randomUUID().toString)
            .setUserContext(sc.UserContext.newBuilder().setUserId("sbt-spark-pipelines"))
            .setSchema(
              sc.AnalyzePlanRequest.Schema
                .newBuilder()
                .setPlan(sc.Plan.newBuilder().setRoot(relation))
            )
            .build()
        )
        .map { response =>
          response.getSchema.getSchema.getStruct.getFieldsList.asScala.toList.map { field =>
            SchemaField(field.getName, field.getDataType.getKindCase.name.toLowerCase)
          }
        }
        .mapError(analysisError)
    }

  /** Ask the analyzer to EXPLAIN a relation — the wire-INTERPRETATION probe.
    *
    * `AnalyzePlan`/`Explain` hands back the planner's own rendering of the plan
    * it decoded from our bytes. Under [[ExplainMode.Extended]] that includes the
    * *Parsed Logical Plan*: `SparkConnectPlanner`'s raw decode, before the
    * analyzer resolves anything. That section is the only user-visible answer to
    * "how does this server read what we send" — which matters because the
    * reading is not a function of the bytes alone: an in-graph table that
    * already exists in the catalog can arrive PRE-RESOLVED, and the pipeline
    * then never sees a read to register a dependency on (see [[PlanDiagnostics]]).
    *
    * Caveat, by design: this is a STANDALONE-session analysis. There is no
    * pipeline rewrite in front of it and catalog resolution is active, so it
    * shows how the server reads the relation, not what the pipeline's own
    * analysis will do with it. That is exactly why the fresh-vs-re-run diff is
    * informative: identical bytes, different catalog, different reading.
    *
    * Deadlined like every other unary RPC here — the transport applies
    * [[TransportConfig.deadlineSeconds]], so a wedged server cannot hang a build.
    *
    * @param sessionId the caller's session, so an explain shares the session
    *                  with the handshake that preceded it (a different session
    *                  is a different server-side state, hence a different answer)
    */
  private[connect] def explainOn(
      relation: sc.Relation,
      mode: ExplainMode,
      sessionId: String,
  ): ZIO[ConnectTransport, RegistrationError, String] =
    ZIO.serviceWithZIO[ConnectTransport] { transport =>
      transport
        .analyze(
          sc.AnalyzePlanRequest
            .newBuilder()
            .setSessionId(sessionId)
            .setUserContext(sc.UserContext.newBuilder().setUserId("sbt-spark-pipelines"))
            .setExplain(
              sc.AnalyzePlanRequest.Explain
                .newBuilder()
                .setPlan(sc.Plan.newBuilder().setRoot(relation))
                .setExplainMode(mode.proto)
            )
            .build()
        )
        .map(_.getExplain.getExplainString)
        .mapError(analysisError)
    }

  /** The (host, port) entry point every call site already uses. A thin
    * forwarder: build the live transport, run [[analyzeSchemaOn]] on it. The
    * whole interaction fits inside this effect, so the LAYER form is the right
    * one here — its scope closes when the effect completes. */
  def analyzeSchema(
      host: String,
      port: Int,
      relation: sc.Relation,
      // Transport security + per-RPC deadline; plaintext/anonymous by default.
      transport: TransportConfig = TransportConfig.plaintext,
  ): IO[RegistrationError, List[SchemaField]] =
    analyzeSchemaOn(relation).provide(ConnectTransport.live(host, port, transport))

  /** Shared failure mapping for every `AnalyzePlan` call here: a gRPC status is
    * the server's verdict, anything else is transport.
    *
    * With one carve-out, the same one `PipelinesRegistration.grpcError` makes:
    * `UNAVAILABLE` means we never reached a server, so it is TRANSPORT, not a
    * verdict. The distinction used to be cosmetic here; it stopped being
    * cosmetic when [[PlanExplain]] started treating a server verdict as
    * information (a table that does not exist yet is an answer) while still
    * failing on transport — without this, a server that is simply down would
    * be reported as a perfectly informative "the server declined to analyze".
    */
  private def analysisError(cause: Throwable): RegistrationError = cause match
    case e: StatusRuntimeException if e.getStatus.getCode == io.grpc.Status.Code.UNAVAILABLE =>
      RegistrationError.TransportFailure(s"server unreachable: ${e.getStatus.getDescription}")
    case e: StatusRuntimeException =>
      RegistrationError.ServerRejected(s"${e.getStatus.getCode}: ${e.getStatus.getDescription}")
    case other => RegistrationError.TransportFailure(other.toString)
