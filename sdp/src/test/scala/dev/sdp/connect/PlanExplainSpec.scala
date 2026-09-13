package dev.sdp.connect

import dev.sdp.core.*
import dev.sdp.core.algebra.{Ex, LitValue, Rel}
import io.grpc.{Status, StatusRuntimeException}
import org.apache.spark.connect.proto as sc
import zio.*
import zio.stream.*
import zio.test.*

import PipelinesRegistration.RegistrationError

/** `sdpExplain` against a SCRIPTED server (the [[ConnectTransport]] seam, P3.2).
  *
  * What has to be true, and could not be observed without the seam:
  *
  *   - the REQUEST shape — an `AnalyzePlanRequest.Explain` carrying the very
  *     relation the encoder would register, in the mode we asked for;
  *   - the handshake runs FIRST, on the same session as the explains;
  *   - a server VERDICT on one flow is data (the report says "declined"),
  *     while a transport failure is a failure. That asymmetry is the whole
  *     exit-code rule, and it is where a diagnostic most easily goes wrong:
  *     failing the build when it finds something is how a diagnostic gets
  *     turned off.
  */
object PlanExplainSpec extends ZIOSpecDefault:

  // ---------------------------------------------------------------- fixtures

  /** `bronze` (external) → `silver` via select, `silver` → `gold` via
    * withColumn: one healthy shape and one at-risk shape in one graph. */
  private val manifest: PipelineManifest =
    PipelineManifest.fromGraphAndFlows(
      PipelineGraph(
        Map(
          "bronze" -> PipelineNode.ExternalTable("bronze"),
          "silver" -> PipelineNode.StreamingTable("silver", "delta"),
          "gold"   -> PipelineNode.Table("gold", "delta"),
        ),
        Set(DependencyEdge("bronze", "silver"), DependencyEdge("silver", "gold")),
      ),
      List(
        Flow(
          "silver",
          "silver",
          FlowDetails.WriteRelation(
            Rel.Project(Rel.NamedTable("bronze", streaming = true), List(Ex.Star(None)))
          ),
        ),
        Flow(
          "gold",
          "gold",
          FlowDetails.WriteRelation(
            Rel.WithColumns(
              Rel.NamedTable("silver", streaming = false),
              List("tag" -> Ex.Lit(LitValue.I32(1))),
            )
          ),
        ),
      ),
    )

  private val explainText =
    """== Parsed Logical Plan ==
      |'Project [*]
      |+- 'UnresolvedRelation [silver], [], false
      |""".stripMargin

  private def explainResponse(text: String): sc.AnalyzePlanResponse =
    sc.AnalyzePlanResponse
      .newBuilder()
      .setExplain(sc.AnalyzePlanResponse.Explain.newBuilder().setExplainString(text))
      .build()

  private def versionResponse(version: String): sc.AnalyzePlanResponse =
    sc.AnalyzePlanResponse
      .newBuilder()
      .setSparkVersion(sc.AnalyzePlanResponse.SparkVersion.newBuilder().setVersion(version))
      .build()

  // ------------------------------------------------------------- the scripted
  //                                                                 transport

  /** Records every `AnalyzePlan` request and answers from a function of it.
    * `execute`/`executeUnbounded` are never reached: an explain registers
    * nothing, and a call to either would be the bug. */
  private final class ScriptedTransport(
      log: Ref[Vector[sc.AnalyzePlanRequest]],
      answerExplain: sc.AnalyzePlanRequest => IO[Throwable, sc.AnalyzePlanResponse],
  ) extends ConnectTransport:
    val endpoint: String = "sc://stub:15002"

    def execute(request: sc.ExecutePlanRequest): ZStream[Any, Throwable, sc.ExecutePlanResponse] =
      ZStream.fail(new IllegalStateException("explain must not execute anything"))

    def executeUnbounded(
        request: sc.ExecutePlanRequest
    ): ZStream[Any, Throwable, sc.ExecutePlanResponse] = execute(request)

    def analyze(request: sc.AnalyzePlanRequest): IO[Throwable, sc.AnalyzePlanResponse] =
      log.update(_ :+ request) *>
        (if request.hasSparkVersion then ZIO.succeed(versionResponse("4.2.0"))
         else answerExplain(request))

    def cancel: UIO[Unit] = ZIO.unit

  private def scripted(
      answerExplain: sc.AnalyzePlanRequest => IO[Throwable, sc.AnalyzePlanResponse]
  ): UIO[(ConnectTransport, UIO[List[sc.AnalyzePlanRequest]])] =
    Ref.make(Vector.empty[sc.AnalyzePlanRequest]).map { ref =>
      (new ScriptedTransport(ref, answerExplain), ref.get.map(_.toList))
    }

  private def run(
      answerExplain: sc.AnalyzePlanRequest => IO[Throwable, sc.AnalyzePlanResponse],
      flowName: Option[String] = None,
      mode: PlanAnalysis.ExplainMode = PlanAnalysis.ExplainMode.Extended,
  ): IO[RegistrationError, (PlanExplain.Report, List[sc.AnalyzePlanRequest])] =
    scripted(answerExplain).flatMap { (transport, requests) =>
      PlanExplain
        .explainOn(
          manifest,
          flowName,
          mode,
          defaultCatalog = Some("warehouse"),
          defaultDatabase = Some("dev_eric"),
          versionCheck = true,
        )
        .provideEnvironment(ZEnvironment[ConnectTransport](transport))
        .zip(requests)
    }

  private def explainsOf(requests: List[sc.AnalyzePlanRequest]): List[sc.AnalyzePlanRequest] =
    requests.filter(_.hasExplain)

  def spec = suite("PlanExplain — the live wire-interpretation probe")(
    test("the request is an Explain of the ENCODER's relation, in the asked-for mode") {
      for
        (report, requests) <- run(_ => ZIO.succeed(explainResponse(explainText)))
        explains = explainsOf(requests)
        // the relation the registration would actually send for `gold`
        expected = PlanExplain.flows(manifest, Some("gold")).head.getRelationFlowDetails.getRelation
      yield assertTrue(
        report.flows.map(_.flowName) == List("gold", "silver"),
        explains.sizeCompare(2) == 0,
        explains.forall(
          _.getExplain.getExplainMode ==
            sc.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_EXTENDED
        ),
        explains.forall(_.getExplain.hasPlan),
        explains.map(_.getExplain.getPlan.getRoot).contains(expected),
        // one session for the handshake AND every explain
        requests.map(_.getSessionId).distinct.sizeCompare(1) == 0,
      )
    },
    test("--formatted asks for EXPLAIN_MODE_FORMATTED") {
      for (_, requests) <- run(
        _ => ZIO.succeed(explainResponse("== Physical Plan ==")),
        mode = PlanAnalysis.ExplainMode.Formatted,
      )
      yield assertTrue(
        explainsOf(requests).forall(
          _.getExplain.getExplainMode ==
            sc.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_FORMATTED
        )
      )
    },
    test("the handshake runs BEFORE the first explain") {
      for (_, requests) <- run(_ => ZIO.succeed(explainResponse(explainText)))
      yield assertTrue(
        requests.head.hasSparkVersion,
        requests.tail.exists(_.hasExplain),
      )
    },
    test("naming a flow explains exactly that one") {
      for
        (report, requests) <- run(
          _ => ZIO.succeed(explainResponse(explainText)),
          flowName = Some("gold"),
        )
      yield assertTrue(
        report.flows.map(_.flowName) == List("gold"),
        explainsOf(requests).sizeCompare(1) == 0,
      )
    },
    test("the classification travels with the report, per flow") {
      for (report, _) <- run(_ => ZIO.succeed(explainResponse(explainText)))
      yield
        val gold = report.flows.find(_.flowName == "gold").get
        assertTrue(
          // silver is in-graph; bronze is an EXTERNAL table and is not classified
          gold.inGraphReads == List("silver"),
          report.flows.find(_.flowName == "silver").get.inGraphReads.isEmpty,
          gold.outcome match
            case PlanExplain.Outcome.Explained(_, reads) =>
              reads.map(_.verdict) == List(PlanDiagnostics.Verdict.Unresolved)
            case _ => false,
        )
    },
    test("a server VERDICT on one flow is reported, not raised — and the rest still run") {
      val notFound = new StatusRuntimeException(
        Status.INTERNAL.withDescription("[TABLE_OR_VIEW_NOT_FOUND] The table or view `silver`…")
      )
      for
        (report, _) <- run(request =>
          if request.getExplain.getPlan.getRoot.hasWithColumns then ZIO.fail(notFound)
          else ZIO.succeed(explainResponse(explainText))
        )
      yield
        val gold   = report.flows.find(_.flowName == "gold").get
        val silver = report.flows.find(_.flowName == "silver").get
        assertTrue(
          gold.outcome match
            case PlanExplain.Outcome.Rejected(detail) => detail.contains("TABLE_OR_VIEW_NOT_FOUND")
            case _                                    => false,
          silver.outcome match
            case _: PlanExplain.Outcome.Explained => true
            case _                                => false,
        )
    },
    test("a TRANSPORT failure fails the effect — there is nothing left to ask") {
      val unavailable =
        new StatusRuntimeException(Status.UNAVAILABLE.withDescription("io exception"))
      run(_ => ZIO.fail(unavailable)).either.map {
        case Left(RegistrationError.TransportFailure(detail)) =>
          assertTrue(detail.contains("unreachable"))
        case other => assertTrue(false) ?? s"expected TransportFailure, got $other"
      }
    },
    test("an AUTO CDC flow is skipped: parameters, not a relation") {
      val cdc = PipelineManifest.fromGraphAndFlows(
        PipelineGraph(Map("dim" -> PipelineNode.StreamingTable("dim", "delta")), Set.empty),
        List(Flow("dim", "dim", FlowDetails.AutoCdc("bronze.cdc", List(Ex.Col("id")), Ex.Col("seq")))),
      )
      scripted(_ => ZIO.succeed(explainResponse(explainText))).flatMap { (transport, requests) =>
        PlanExplain
          .explainOn(cdc, None, PlanAnalysis.ExplainMode.Extended, None, None, versionCheck = false)
          .provideEnvironment(ZEnvironment[ConnectTransport](transport))
          .zip(requests)
          .map { (report, sent) =>
            assertTrue(
              report.flows.map(_.outcome).forall {
                case _: PlanExplain.Outcome.Skipped => true
                case _                              => false
              },
              // nothing was asked of the server at all
              explainsOf(sent).isEmpty,
            )
          }
      }
    },
    suite("offline surface")(
      test("flowNames lists every flow the encoder would send, view flows included") {
        val withView = PipelineManifest.fromGraphAndFlows(
          PipelineGraph(
            Map("daily" -> PipelineNode.MaterializedView("daily", "SELECT 1")),
            Set.empty,
          ),
          Nil,
        )
        assertTrue(
          PlanExplain.flowNames(manifest) == List("gold", "silver"),
          PlanExplain.flowNames(withView) == List("daily"),
        )
      },
      test("an unknown flow name is refused offline, naming the ones that exist") {
        assertTrue(
          PlanExplain.checkFlowName(manifest, None).isRight,
          PlanExplain.checkFlowName(manifest, Some("gold")).isRight,
          PlanExplain
            .checkFlowName(manifest, Some("platinum"))
            .swap
            .exists(m => m.contains("unknown flow 'platinum'") && m.contains("gold, silver")),
        )
      },
      test("render prints the plan, what it shows, and the one factual upstream note") {
        val report = PlanExplain.Report(
          "sc://stub:15002",
          PlanAnalysis.ExplainMode.Extended,
          Some("warehouse"),
          Some("dev_eric"),
          Some("4.2.0"),
          List(
            PlanExplain.FlowReport(
              "gold",
              "gold",
              List("silver"),
              PlanExplain.Outcome.Explained(
                explainText,
                List(
                  PlanDiagnostics.ReadClassification(
                    "silver",
                    PlanDiagnostics.Verdict.PreResolved("RelationV2"),
                    None,
                  )
                ),
              ),
            )
          ),
        )
        val text = PlanExplain.render(report).mkString("\n")
        assertTrue(
          text.contains("sc://stub:15002"),
          text.contains("Spark 4.2.0"),
          text.contains("catalog=warehouse"),
          text.contains("database=dev_eric"),
          text.contains("flow 'gold' → gold (in-graph reads: silver)"),
          text.contains("'UnresolvedRelation [silver]"), // the plan, verbatim
          text.contains("HEURISTIC"),
          text.contains("read 'silver': already resolved in Parsed plan (RelationV2)"),
          // exactly one accompanying note, and it prescribes nothing
          text.contains(s"note: ${PlanDiagnostics.PreResolvedNote}"),
          !text.contains("prefer select"),
        )
      },
      test("render says so when nothing matched") {
        val empty = PlanExplain.Report(
          "sc://stub:15002",
          PlanAnalysis.ExplainMode.Extended,
          None,
          None,
          None,
          Nil,
        )
        assertTrue(PlanExplain.render(empty).exists(_.contains("no flows matched")))
      },
    ),
  )
