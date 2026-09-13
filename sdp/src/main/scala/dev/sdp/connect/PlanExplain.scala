package dev.sdp.connect

import java.util.UUID

import dev.sdp.core.{Flow, FlowDetails, PipelineManifest, PipelineNode}
import org.apache.spark.connect.proto as sc
import zio.*

import PipelinesRegistration.RegistrationError

/** **The live half of the plan diagnostics**: ask the server to explain each
  * flow's relation, then say what the answer means for the dependency edges.
  *
  * [[WireDump]] writes down what we SAID. This reads back how the server
  * INTERPRETS it — the other end of a wire that is otherwise a one-way mirror
  * (the server logs nothing about received plans, and `pipelines.proto` has no
  * graph-readback command, so the graph it built cannot be inspected at all).
  *
  * Three deliberate design choices:
  *
  *   - **The relations explained are the encoder's own.** The flows come from
  *     [[PipelineProtoEncoder.definitions]], exactly as a registration would
  *     produce them — including the SQL flows derived for materialized views —
  *     so the thing explained is the thing sent. A second, "equivalent" encode
  *     path would prove nothing.
  *   - **A server rejection is INFORMATION, not a failure.** Explaining a flow
  *     against a clean catalog legitimately fails with `TABLE_OR_VIEW_NOT_FOUND`
  *     — that answer IS the diagnosis (nothing is pre-resolved because nothing
  *     exists yet). So a `ServerRejected` becomes [[Outcome.Rejected]] and the
  *     command still exits 0; only a transport failure fails the effect.
  *   - **One session, one handshake.** The version handshake runs first, on the
  *     same session as the explains, exactly as `PipelinesRegistration` does.
  */
object PlanExplain:

  /** What came back for one flow. */
  enum Outcome:
    /** The server explained the plan; `reads` is the heuristic's verdict per
      * in-graph read (see [[PlanDiagnostics]]). */
    case Explained(plan: String, reads: List[PlanDiagnostics.ReadClassification])

    /** The server refused to analyze this plan — most often because a table
      * does not exist yet. Informative, not an error. */
    case Rejected(message: String)

    /** Nothing to explain: an AUTO CDC flow carries parameters, not a relation. */
    case Skipped(reason: String)

  /** One flow's report. */
  final case class FlowReport(
      flowName: String,
      target: String,
      inGraphReads: List[String],
      outcome: Outcome,
  )

  /** The whole explain run. `serverVersion` is what the handshake learned (None
    * when the check was skipped or the probe failed). */
  final case class Report(
      endpoint: String,
      mode: PlanAnalysis.ExplainMode,
      defaultCatalog: Option[String],
      defaultDatabase: Option[String],
      serverVersion: Option[String],
      flows: List[FlowReport],
  )

  /** Explain every flow of `manifest` (or just `flowName`) against the server.
    *
    * The `(host, port)` entry point: the whole interaction fits in one effect,
    * so the transport is a LAYER whose scope closes when the effect completes
    * (the same shape [[PlanAnalysis.analyzeSchema]] uses).
    *
    * @param flowName explain only this flow; `None` = all of them
    */
  def explain(
      host: String,
      port: Int,
      manifest: PipelineManifest,
      flowName: Option[String] = None,
      mode: PlanAnalysis.ExplainMode = PlanAnalysis.ExplainMode.Extended,
      defaultCatalog: Option[String] = None,
      defaultDatabase: Option[String] = None,
      transport: TransportConfig = TransportConfig.plaintext,
      versionCheck: Boolean = true,
  ): IO[RegistrationError, Report] =
    explainOn(manifest, flowName, mode, defaultCatalog, defaultDatabase, versionCheck)
      .provide(ConnectTransport.live(host, port, transport))

  /** The same thing against the [[ConnectTransport]] seam — drivable by a stub
    * transport offline, which is how the request shape is tested. */
  private[connect] def explainOn(
      manifest: PipelineManifest,
      flowName: Option[String],
      mode: PlanAnalysis.ExplainMode,
      defaultCatalog: Option[String],
      defaultDatabase: Option[String],
      versionCheck: Boolean,
  ): ZIO[ConnectTransport, RegistrationError, Report] =
    ZIO.serviceWithZIO[ConnectTransport] { transport =>
      val sessionId = UUID.randomUUID().toString
      val selected  = flows(manifest, flowName)

      for
        // The C1 handshake, first and on this session — the same body every
        // live task runs (see PipelinesRegistration.handshake).
        _       <- PipelinesRegistration.handshake(transport, sessionId, manifest, versionCheck)
        version <- reportedVersion(transport, sessionId, versionCheck)
        reports <- ZIO.foreach(selected)(explainFlow(_, manifest, mode, sessionId))
      yield Report(
        transport.endpoint,
        mode,
        defaultCatalog,
        defaultDatabase,
        version,
        reports,
      )
    }

  /** The flows to explain, as the ENCODER produces them: authored relation
    * flows, AUTO CDC flows, and the SQL flows derived for materialized and
    * temporary views. Filtered by name when one was asked for; an unknown name
    * yields an empty list, which the caller reports as such.
    *
    * The graph id is irrelevant to an explain, so the [[WireDump]] placeholder
    * stands in — nothing here registers anything.
    */
  private[connect] def flows(
      manifest: PipelineManifest,
      flowName: Option[String],
  ): List[sc.PipelineCommand.DefineFlow] =
    PipelineProtoEncoder
      .definitions(WireDump.GraphIdPlaceholder, manifest)
      .filter(_.hasDefineFlow)
      .map(_.getDefineFlow)
      .filter(f => flowName.forall(_ == f.getFlowName))

  /** Every flow name a caller may ask for, in send order — the tab-completion
    * source and the "did you mean" list. */
  def flowNames(manifest: PipelineManifest): List[String] =
    flows(manifest, None).map(_.getFlowName)

  /** Reject an unknown flow name BEFORE a channel is opened, naming the ones
    * that exist. Offline and total: a typo is a user error, and answering it
    * with "(no flows matched)" after a round trip is a worse answer than
    * answering it instantly with the list. */
  def checkFlowName(manifest: PipelineManifest, flowName: Option[String]): Either[String, Unit] =
    flowName match
      case None => Right(())
      case Some(wanted) =>
        val available = flowNames(manifest)
        if available.contains(wanted) then Right(())
        else
          Left(
            s"unknown flow '$wanted'. Flows in this pipeline: ${available.mkString(", ")}"
          )

  private def explainFlow(
      flow: sc.PipelineCommand.DefineFlow,
      manifest: PipelineManifest,
      mode: PlanAnalysis.ExplainMode,
      sessionId: String,
  ): ZIO[ConnectTransport, RegistrationError, FlowReport] =
    val name   = flow.getFlowName
    val target = flow.getTargetDatasetName
    val reads  = inGraphReads(manifest, name, target)

    if !flow.hasRelationFlowDetails then
      ZIO.succeed(
        FlowReport(
          name,
          target,
          reads,
          Outcome.Skipped("AUTO CDC flow — parameters, not a relation, so there is no plan to explain"),
        )
      )
    else
      PlanAnalysis
        .explainOn(flow.getRelationFlowDetails.getRelation, mode, sessionId)
        .map(plan => FlowReport(name, target, reads, Outcome.Explained(plan, PlanDiagnostics.classify(plan, reads))))
        .catchSome {
          // The server's verdict on ONE flow is an outcome, not the end of the
          // run: a clean catalog rejects half the graph and that is the answer,
          // not a defect. Transport failures still fail — there is nothing left
          // to ask.
          case RegistrationError.ServerRejected(detail) =>
            ZIO.succeed(FlowReport(name, target, reads, Outcome.Rejected(detail)))
        }

  /** Reads of this flow that are datasets the PIPELINE produces.
    *
    * External tables are excluded on purpose: they live in the catalog by
    * definition, so seeing one pre-resolved is expected and carries no risk —
    * flagging it would be noise that trains people to ignore the signal.
    *
    * The flow's own reads are used when it is an authored flow; otherwise (a
    * derived or view flow) the manifest's edges into the target are the honest
    * answer.
    */
  private[connect] def inGraphReads(
      manifest: PipelineManifest,
      flowName: String,
      target: String,
  ): List[String] =
    val managed: Set[String] = manifest.nodes.filter {
      case _: PipelineNode.ExternalTable => false
      case _                             => true
    }.map(_.id).toSet

    val raw = manifest.flows.find(_.name == flowName) match
      case Some(flow) => Flow.reads(flow)
      case None       => manifest.edges.filter(_.to == target).map(_.from).toSet

    raw.filter(managed.contains).toList.sorted

  /** The server version, for the header line. Re-asks rather than threading it
    * out of the handshake: it is one of the cheapest round trips Spark Connect
    * has, and a failure here is cosmetic (the handshake already decided whether
    * the run may proceed), so it degrades to `None` instead of failing. */
  private def reportedVersion(
      transport: ConnectTransport,
      sessionId: String,
      versionCheck: Boolean,
  ): UIO[Option[String]] =
    if !versionCheck then ZIO.none
    else PlanAnalysis.sparkVersionOn(transport, sessionId).option

  // ------------------------------------------------------------------
  // rendering
  // ------------------------------------------------------------------

  /** The report as console lines — ONE renderer, so `sdpExplain` and
    * `sdp explain` print the same thing. Returned as lines (not one string)
    * because the sbt plugin logs line-by-line and stdout does not.
    */
  def render(report: Report): List[String] =
    val header = List(
      s"explain (${report.mode.label}) on ${report.endpoint}" +
        report.serverVersion.fold("")(v => s" — Spark $v"),
      s"  graph defaults in effect: catalog=${report.defaultCatalog.getOrElse("<omitted>")}, " +
        s"database=${report.defaultDatabase.getOrElse("<omitted>")}",
      "  NOTE: this is a standalone-session analysis — the catalog resolves names here, with no " +
        "pipeline rewrite in front of it. That is the point: it shows how THIS server, with THIS " +
        "catalog state, reads the bytes we send.",
    )

    if report.flows.isEmpty then header :+ "  (no flows matched)"
    else header ::: report.flows.flatMap(renderFlow)

  private def renderFlow(flow: FlowReport): List[String] =
    val title =
      s"flow '${flow.flowName}' → ${flow.target}" +
        (if flow.inGraphReads.isEmpty then " (no in-graph reads)"
         else s" (in-graph reads: ${flow.inGraphReads.mkString(", ")})")

    val body = flow.outcome match
      case Outcome.Skipped(reason) => List(s"  skipped: $reason")
      case Outcome.Rejected(detail) =>
        List(
          s"  the server declined to analyze this plan: $detail",
          "  (informative, not a failure — on a clean catalog nothing is resolvable yet, which is " +
            "itself the answer: no in-graph read can be pre-resolved before its upstream exists)",
        )
      case Outcome.Explained(plan, reads) =>
        plan.linesIterator.map("  " + _).toList ::: classificationLines(reads)

    ("" :: title :: body)

  /** What the parsed plan shows for each in-graph read, and — when one arrived
    * already resolved — the ONE neutral sentence naming the upstream behavior.
    * No recommendation: this project reports what the server did (see
    * [[PlanDiagnostics.PreResolvedNote]]). */
  private def classificationLines(
      reads: List[PlanDiagnostics.ReadClassification]
  ): List[String] =
    if reads.isEmpty then Nil
    else
      val verdicts = reads.map(r => s"  ${r.render}")
      val preResolved = reads.exists(_.verdict match
        case _: PlanDiagnostics.Verdict.PreResolved => true
        case _                                      => false
      )
      val note = if preResolved then List(s"  note: ${PlanDiagnostics.PreResolvedNote}") else Nil
      ("  --- in-graph reads, as the Parsed Logical Plan above shows them (HEURISTIC) ---" ::
        verdicts) ::: note
