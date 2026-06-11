package dev.sdp.plugin.conformance

import java.nio.charset.StandardCharsets.UTF_8

import zio.test.*

/** F10: the conformance harness's own gates.
  *
  *   1. **Drift gate** — the live artifact's descriptor inventory must match
  *      the checked-in snapshot byte-for-byte. An artifact upgrade fails
  *      here; regenerating the snapshot (see [[RenderInventory]]) turns the
  *      upstream change into a reviewable git diff.
  *   2. **Total relation triage** — every `Relation` oneof entry must be
  *      classified into a tier. New upstream relation types cannot slip in
  *      untriaged.
  *   3. **Claim verification** — every supported-capability claim must name
  *      a real field in the inventory. Upstream renames break claims loudly.
  */
object ConformanceSpec extends ZIOSpecDefault:

  private val SnapshotResource = "/spark-connect-inventory.txt"

  def spec = suite("Spark Connect conformance (F10)")(
    test("drift gate: live descriptor inventory matches the golden snapshot") {
      val stream = Option(getClass.getResourceAsStream(SnapshotResource))
      val snapshot = stream.map(s => new String(s.readAllBytes(), UTF_8))
      val live     = ConnectInventory.render
      assertTrue(
        snapshot.isDefined,
        snapshot.contains(live),
      ) ?? s"Inventory drift detected (or snapshot missing). If this follows a deliberate Spark artifact upgrade, regenerate: sbt 'sbtSparkPipelines/Test/runMain dev.sdp.plugin.conformance.RenderInventory sbt-spark-pipelines/src/test/resources${SnapshotResource}' and review the git diff."
    },
    test("every Relation oneof entry is triaged into a tier") {
      assertTrue(ConnectTiers.untriagedRelationEntries.isEmpty) ??
        s"Untriaged relation entries: ${ConnectTiers.untriagedRelationEntries.mkString(", ")} — classify them in ConnectTiers.relationTriage."
    },
    test("every supported-capability claim names a real field in the inventory") {
      val inventory = ConnectInventory.allCapabilityIds
      val phantom   = SupportedCapabilities.claims.filterNot(inventory.contains)
      assertTrue(phantom.isEmpty) ??
        s"Claims with no matching wire capability (upstream rename?): ${phantom.mkString(", ")}"
    },
    test("the coverage report renders (printed for visibility)") {
      val report = SupportedCapabilities.report
      println("\n" + report + "\n")
      assertTrue(
        report.contains("T0Core"),
        report.contains("untriaged expression entries"),
      )
    },
  )
