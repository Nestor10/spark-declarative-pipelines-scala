package dev.sdp.connect.conformance

import zio.test.*

/** The drift guard on the behavioral matrix ([[BehaviorInventory]]).
  *
  * A hand-written coverage table rots in two directions, and both are worse
  * than having no table: a row can lose its evidence (an anchor that no longer
  * names anything, a `Covered` row whose spec was renamed or deleted), or a
  * status can quietly drift (a spec is removed and the row keeps claiming it,
  * or new coverage lands and nobody notices the matrix still says Uncovered).
  *
  * So: every row must cite an anchor, every `Covered` row must name a spec
  * class that actually resolves on the test classpath, and the full
  * id → status mapping is asserted against a literal snapshot — changing a
  * status is a deliberate edit in the same commit that changes the coverage.
  */
object BehaviorCoverageSpec extends ZIOSpecDefault:

  import BehaviorInventory.{Area, Behavior, Coverage}

  /** A zio-test spec is an `object`, so the JVM name carries a trailing `$`.
    * Resolve both spellings so the check works for classes too. */
  private def resolves(fqn: String): Boolean =
    val loader = getClass.getClassLoader
    try { val _ = Class.forName(s"$fqn$$", false, loader); true }
    catch
      case _: ClassNotFoundException =>
        try { val _ = Class.forName(fqn, false, loader); true }
        catch case _: ClassNotFoundException => false

  /** The frozen snapshot: every behavior id and its status (45 rows — 8 covered,
    * 5 not applicable, 32 honest gaps). A coverage change must be reflected here
    * in the same commit that changes it. */
  private val expectedSnapshot: List[(String, String)] = List(
    "REG-1-command-envelope"         -> "Covered(PipelinesRegistrationIntegrationSpec)",
    "REG-2-graph-id"                 -> "Covered(PipelinesRegistrationIntegrationSpec)",
    "REG-3-unknown-graph"            -> "Uncovered",
    "REG-4-output-before-flow"       -> "Uncovered",
    "REG-5-flow-name-shape"          -> "Uncovered",
    "REG-6-duplicates-late"          -> "Uncovered",
    "REG-7-empty-pipeline"           -> "Uncovered",
    "REG-8-storage-required"         -> "Uncovered",
    "REG-9-details-required"         -> "Covered(VersionGateSpec)",
    "REG-10-flow-body-analysis"      -> "NotApplicable",
    "REG-11-unimplemented-commands"  -> "NotApplicable",
    "DEF-1-fallback-mutates-session" -> "Uncovered",
    "DEF-2-four-consumers"           -> "Uncovered",
    "DEF-3-edge-drop"                -> "Uncovered",
    "DEF-4-views-not-inputs"         -> "Uncovered",
    "MAT-1-create"                   -> "Uncovered",
    "MAT-2-alter-only-on-rerun"      -> "Uncovered",
    "MAT-3-props-never-removed"      -> "Uncovered",
    "MAT-4-schema-merge-vs-replace"  -> "Uncovered",
    "MAT-5-partitioning"             -> "NotApplicable",
    "MAT-6-views-republished"        -> "Uncovered",
    "FR-1-mv-truncate-every-run"     -> "Uncovered",
    "FR-2-checkpoint-roll"           -> "Uncovered",
    "FR-3-selection-rules"           -> "Uncovered",
    "FR-4-reset-allowed"             -> "Uncovered",
    "ONCE-1-server-rejects"          -> "Uncovered",
    "ONCE-2-presence-not-value"      -> "Uncovered",
    "ONCE-3-dormant-engine"          -> "NotApplicable",
    "EXT-1-external-resolution"      -> "Uncovered",
    "EXT-2-missing-external"         -> "Covered(PipelinesRegistrationIntegrationSpec)",
    "EXT-3-in-graph-virtual"         -> "Uncovered",
    "EXT-4-cycles"                   -> "NotApplicable",
    "STR-1-available-now"            -> "Uncovered",
    "STR-2-checkpoint-resume"        -> "Uncovered",
    "STR-3-termination"              -> "Uncovered",
    "STR-4-flow-retries"             -> "Uncovered",
    "EV-1-text-only"                 -> "Covered(RunProgressSpec)",
    "EV-2-flow-wording"              -> "Covered(RunProgressSpec)",
    "EV-3-unparsed-states"           -> "Uncovered",
    "EV-4-no-terminal-failure-event" -> "Covered(PipelinesRegistrationIntegrationSpec)",
    "EV-5-dropped-events"            -> "Uncovered",
    "EV-6-ordering"                  -> "Uncovered",
    "DRY-1-validate-only"            -> "Covered(PipelinesRegistrationIntegrationSpec)",
    "DRY-2-indistinguishable"        -> "Uncovered",
    "DRY-3-validation-set"           -> "Uncovered",
  )

  def spec = suite("SDP behavioral conformance matrix (C3)")(
    test("every row cites a Spark source anchor naming a file and a symbol") {
      val bad = BehaviorInventory.rows.filterNot { r =>
        r.anchor.contains(".scala") && r.anchor.contains("→")
      }
      assertTrue(bad.isEmpty) ??
        s"rows whose anchor does not name a file and a symbol: ${bad.map(_.id).mkString(", ")}"
    },
    test("behavior ids are unique and non-empty") {
      val ids = BehaviorInventory.rows.map(_.id)
      assertTrue(ids.distinct.size == ids.size, ids.forall(_.nonEmpty))
    },
    test("every Covered row names a spec class that exists on the test classpath") {
      val phantom = BehaviorInventory.claimedSpecs.filterNot(resolves)
      assertTrue(phantom.isEmpty) ??
        s"Covered rows naming specs that do not resolve (renamed? deleted?): ${phantom.mkString(", ")}"
    },
    test("NotApplicable always carries a reason; Uncovered never pretends") {
      val rows = BehaviorInventory.rows
      val naWithoutReason = rows.collect {
        case b @ Behavior(_, _, _, _, Coverage.NotApplicable(reason), _) if reason.trim.isEmpty => b.id
      }
      val coveredWithoutHow = rows.collect {
        case b @ Behavior(_, _, _, _, Coverage.Covered(_, how), _) if how.trim.isEmpty => b.id
      }
      assertTrue(naWithoutReason.isEmpty, coveredWithoutHow.isEmpty)
    },
    test("the id → status snapshot matches (a coverage change is a deliberate edit)") {
      assertTrue(BehaviorInventory.statusSnapshot == expectedSnapshot) ??
        s"""behavioral coverage drifted. Actual snapshot (paste into expectedSnapshot if intended):
           |${BehaviorInventory.statusSnapshot
            .map { case (id, st) => s"""    "$id" -> "$st",""" }
            .mkString("\n")}""".stripMargin
    },
    test("every area is represented (an empty area means an un-enumerated surface)") {
      val missing = Area.values.toList.filterNot(a => BehaviorInventory.rows.exists(_.area == a))
      assertTrue(missing.isEmpty) ?? s"areas with no rows: ${missing.mkString(", ")}"
    },
    test("the report renders, and renders honestly (printed for visibility)") {
      val report = BehaviorInventory.report
      println("\n" + report + "\n")
      assertTrue(
        report.contains("SDP behavioral conformance matrix"),
        report.contains("TOTAL"),
        // the uncovered line must actually list ids while gaps exist
        report.contains("uncovered:"),
      )
    },
  )
