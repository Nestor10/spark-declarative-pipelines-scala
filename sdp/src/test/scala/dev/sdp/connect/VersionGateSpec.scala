package dev.sdp.connect

import dev.sdp.core.*
import dev.sdp.core.algebra.{Ex, Rel}
import zio.test.*

import PipelinesRegistration.RegistrationError

/** The server-version handshake's DECISION, tested as a pure function — no
  * channel, no container. The I/O half (one `AnalyzePlan`/`SparkVersion` round
  * trip) is a recipe already proven against live servers; the part that can be
  * *wrong* is the policy, and it is all here:
  *
  *   - the version × construct matrix (who needs what),
  *   - lenient parsing (patch levels, previews, vendor suffixes),
  *   - the two asymmetric failure modes: unreadable ⇒ proceed, too old ⇒ refuse.
  */
object VersionGateSpec extends ZIOSpecDefault:

  import VersionGate.{Construct, ServerVersion}

  private val endpoint = "sc://spark.example:15002"

  /** A plain v2-shape pipeline: one streaming table fed by a relation flow.
    * Nothing in it is version-gated, so every server can run it. */
  private val plainManifest: PipelineManifest =
    PipelineManifest.fromGraphAndFlows(
      PipelineGraph(Map("silver" -> PipelineNode.StreamingTable("silver", "delta")), Set.empty),
      List(
        Flow(
          "silver_flow",
          "silver",
          FlowDetails.WriteRelation(Rel.NamedTable("bronze", streaming = true)),
        )
      ),
    )

  /** An AUTO CDC pipeline — the one gated construct today (needs Spark 4.2+). */
  private def cdcManifest(flowName: String = "dim_auto_cdc"): PipelineManifest =
    PipelineManifest.fromGraphAndFlows(
      PipelineGraph(Map("dim" -> PipelineNode.StreamingTable("dim", "delta")), Set.empty),
      List(
        Flow(
          flowName,
          "dim",
          FlowDetails.AutoCdc("bronze.cdc", List(Ex.Col("id")), Ex.Col("seq")),
        )
      ),
    )

  /** Two AUTO CDC flows: the message must name BOTH, so one run fixes both. */
  private val twoCdcManifest: PipelineManifest =
    PipelineManifest.fromGraphAndFlows(
      PipelineGraph(
        Map(
          "dim_a" -> PipelineNode.StreamingTable("dim_a", "delta"),
          "dim_b" -> PipelineNode.StreamingTable("dim_b", "delta"),
        ),
        Set.empty,
      ),
      List(
        Flow("a_cdc", "dim_a", FlowDetails.AutoCdc("bronze.a", List(Ex.Col("id")), Ex.Col("seq"))),
        Flow("b_cdc", "dim_b", FlowDetails.AutoCdc("bronze.b", List(Ex.Col("id")), Ex.Col("seq"))),
      ),
    )

  def spec = suite("VersionGate (pure server-version handshake)")(
    suite("lenient version parsing")(
      test("release, patch, preview and vendor spellings all resolve to major.minor") {
        assertTrue(
          ServerVersion.parse("4.1.1") == Some(ServerVersion(4, 1)),
          ServerVersion.parse("4.2.0") == Some(ServerVersion(4, 2)),
          ServerVersion.parse("4.2") == Some(ServerVersion(4, 2)),
          // qualifiers and vendor suffixes are noise to a major.minor gate
          ServerVersion.parse("4.2.0-preview1") == Some(ServerVersion(4, 2)),
          ServerVersion.parse("4.2.0-rc1") == Some(ServerVersion(4, 2)),
          ServerVersion.parse("4.1.0-databricks") == Some(ServerVersion(4, 1)),
          ServerVersion.parse("3.5.1-amzn-0") == Some(ServerVersion(3, 5)),
          ServerVersion.parse("4.3.0-SNAPSHOT") == Some(ServerVersion(4, 3)),
          // surrounding whitespace and a stray 'v' are tolerated
          ServerVersion.parse("  4.2.0\n") == Some(ServerVersion(4, 2)),
          ServerVersion.parse("v4.2.0") == Some(ServerVersion(4, 2)),
          // two-digit components are not truncated
          ServerVersion.parse("10.11.0") == Some(ServerVersion(10, 11)),
        )
      },
      test("a string with no leading major.minor is unreadable (not 'old')") {
        assertTrue(
          ServerVersion.parse("") == None,
          ServerVersion.parse("   ") == None,
          ServerVersion.parse("unknown") == None,
          ServerVersion.parse("4") == None,
          // a version must be at the FRONT: a prose string is not a version
          ServerVersion.parse("built from 4.2.0") == None,
        )
      },
      test("ordering compares major then minor") {
        val ord = summon[Ordering[ServerVersion]]
        assertTrue(
          ord.lt(ServerVersion(4, 1), ServerVersion(4, 2)),
          ord.lt(ServerVersion(3, 9), ServerVersion(4, 0)),
          ord.gt(ServerVersion(4, 10), ServerVersion(4, 9)),
          ord.equiv(ServerVersion(4, 2), ServerVersion(4, 2)),
        )
      },
    ),
    suite("the requirements map")(
      test("AUTO CDC (SCD type 1) requires Spark 4.2") {
        assertTrue(Construct.AutoCdcScd1.minimumVersion == ServerVersion(4, 2))
      },
      test("every gated construct names itself in prose (the error is read by humans)") {
        assertTrue(Construct.values.forall(c => c.label.nonEmpty && c.label.exists(_.isLetter)))
      },
      test("only AUTO CDC flows are gated — a WriteRelation pipeline uses nothing") {
        assertTrue(
          VersionGate.constructsOf(plainManifest).isEmpty,
          VersionGate.constructsOf(cdcManifest()).map(_.construct) == List(Construct.AutoCdcScd1),
        )
      },
    ),
    suite("version × construct verdicts")(
      test("an ungated pipeline is accepted by every server, however ancient") {
        assertTrue(
          List("3.5.1", "4.0.0", "4.1.1", "4.2.0", "5.0.0")
            .forall(v => VersionGate.check(v, plainManifest, endpoint).isRight)
        )
      },
      test("AUTO CDC is accepted on 4.2 and newer") {
        assertTrue(
          List("4.2.0", "4.2.3", "4.3.0", "5.0.0", "4.2.0-preview1")
            .forall(v => VersionGate.check(v, cdcManifest(), endpoint).isRight)
        )
      },
      test("AUTO CDC is refused on anything older than 4.2") {
        assertTrue(
          List("4.1.1", "4.1.2", "4.0.0", "3.5.1")
            .forall(v => VersionGate.check(v, cdcManifest(), endpoint).isLeft)
        )
      },
    ),
    suite("the refusal message")(
      test("names the construct, the flow, the requirement, the endpoint and what the server said") {
        val detail = VersionGate.check("4.1.1", cdcManifest("orders_cdc"), endpoint) match
          case Left(RegistrationError.ServerTooOld(d)) => d
          case other                                   => s"UNEXPECTED: $other"
        assertTrue(
          detail.contains("AUTO CDC flow (SCD type 1)"),
          detail.contains("orders_cdc"),
          detail.contains("target 'dim'"),
          detail.contains("Spark 4.2+ server"),
          detail.contains(endpoint),
          detail.contains("reports 4.1.1"),
        )
      },
      test("every offending flow is listed, so one run fixes them all") {
        val detail = VersionGate.check("4.1.1", twoCdcManifest, endpoint) match
          case Left(RegistrationError.ServerTooOld(d)) => d
          case other                                   => s"UNEXPECTED: $other"
        assertTrue(detail.contains("a_cdc"), detail.contains("b_cdc"))
      },
      test("it renders through the RegistrationError path as one readable sentence") {
        val rendered = VersionGate.check("4.1.1", cdcManifest(), endpoint) match
          case Left(err) => err.describe
          case Right(_)  => "UNEXPECTED: accepted"
        assertTrue(
          rendered.startsWith("Spark Connect server is too old for this pipeline:"),
          // a sentence, not a stack trace
          !rendered.contains("Exception"),
          !rendered.contains("\tat "),
        )
      },
    ),
    suite("unreadable versions never block (forks report odd strings)")(
      test("an unparseable version proceeds even for a gated construct") {
        assertTrue(
          List("", "unknown", "custom-build", "4")
            .forall(v => VersionGate.check(v, cdcManifest(), endpoint).isRight)
        )
      }
    ),
  )
