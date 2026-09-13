package dev.sdp.connect

import zio.test.*

/** The dependency-edge TELL, over recorded `EXPLAIN EXTENDED` output.
  *
  * The fixtures are the two shapes that actually matter, written the way Spark
  * renders them:
  *
  *   - a read that arrives as a NAME (`'UnresolvedRelation`) — the pipeline
  *     will see it and register the dependency;
  *   - a read that arrives already BOUND, either as a `RelationV2` (a V2
  *     catalog) or as `SubqueryAlias` + `Relation … parquet` (the V1 path) —
  *     the name is gone before the pipeline ever looks, and the edge silently
  *     is not registered.
  *
  * The sharpest test in here is the one that looks like a formality: an
  * unresolved parsed plan whose *Analyzed* section is full of `SubqueryAlias`
  * and `Relation … parquet` for the same table. Classifying that as
  * pre-resolved would be a false alarm on every healthy pipeline, so section
  * extraction is load-bearing, not cosmetic.
  */
object PlanDiagnosticsSpec extends ZIOSpecDefault:

  /** A healthy flow: `spark.read.table("silver").select(...)`. The parsed plan
    * still has the name; resolution happens later, in the Analyzed section. */
  private val unresolvedExplain =
    """== Parsed Logical Plan ==
      |'Project [*]
      |+- 'UnresolvedRelation [silver], [], false
      |
      |== Analyzed Logical Plan ==
      |id: bigint, amount: double
      |Project [id#12L, amount#13]
      |+- SubqueryAlias spark_catalog.default.silver
      |   +- Relation spark_catalog.default.silver[id#12L,amount#13] parquet
      |
      |== Optimized Logical Plan ==
      |Relation spark_catalog.default.silver[id#12L,amount#13] parquet
      |
      |== Physical Plan ==
      |*(1) ColumnarToRow
      |+- FileScan parquet spark_catalog.default.silver[id#12L,amount#13]
      |""".stripMargin

  /** The bug's shape on a V2 catalog: `withColumn` made the planner analyze its
    * child while decoding, so the PARSED plan already carries a RelationV2. */
  private val preResolvedV2Explain =
    """== Parsed Logical Plan ==
      |'Project [id#12L, amount#13, 1 AS tag#20]
      |+- RelationV2[id#12L, amount#13] warehouse.dev_eric.silver warehouse.dev_eric.silver
      |
      |== Analyzed Logical Plan ==
      |id: bigint, amount: double, tag: int
      |Project [id#12L, amount#13, 1 AS tag#20]
      |+- RelationV2[id#12L, amount#13] warehouse.dev_eric.silver warehouse.dev_eric.silver
      |""".stripMargin

  /** The same bug on the V1 path: the name is on the `SubqueryAlias`, the
    * interesting node is the `Relation … parquet` underneath it. */
  private val preResolvedV1Explain =
    """== Parsed Logical Plan ==
      |Project [id#12L, 1 AS tag#20]
      |+- SubqueryAlias spark_catalog.default.silver
      |   +- Relation spark_catalog.default.silver[id#12L,amount#13] parquet
      |
      |== Analyzed Logical Plan ==
      |id: bigint, tag: int
      |Project [id#12L, 1 AS tag#20]
      |+- SubqueryAlias spark_catalog.default.silver
      |   +- Relation spark_catalog.default.silver[id#12L,amount#13] parquet
      |""".stripMargin

  /** A multipart name, rendered the way an unresolved read spells it. */
  private val multipartExplain =
    """== Parsed Logical Plan ==
      |'Project [*]
      |+- 'UnresolvedRelation [bronze, orders], [], false
      |""".stripMargin

  /** FORMATTED mode: no Parsed Logical Plan section at all. */
  private val formattedExplain =
    """== Physical Plan ==
      |* Project (2)
      |+- Scan parquet spark_catalog.default.silver (1)
      |""".stripMargin

  private def verdictOf(explain: String, read: String): PlanDiagnostics.Verdict =
    PlanDiagnostics.classify(explain, List(read)).head.verdict

  def spec = suite("PlanDiagnostics — the dependency-edge tell")(
    suite("section extraction")(
      test("the parsed section stops at the next == header ==") {
        val section = PlanDiagnostics.parsedLogicalPlan(unresolvedExplain)
        assertTrue(
          section.exists(_.contains("'UnresolvedRelation [silver]")),
          // everything after the header belongs to another section
          section.exists(!_.contains("Analyzed Logical Plan")),
          section.exists(!_.contains("SubqueryAlias")),
          section.exists(!_.contains("FileScan")),
        )
      },
      test("a mode without a parsed section yields None, and classifies nothing") {
        assertTrue(
          PlanDiagnostics.parsedLogicalPlan(formattedExplain).isEmpty,
          verdictOf(formattedExplain, "silver") == PlanDiagnostics.Verdict.NotFound,
        )
      },
    ),
    suite("classification")(
      test("an UnresolvedRelation read means the pipeline will register the dependency") {
        assertTrue(verdictOf(unresolvedExplain, "silver") == PlanDiagnostics.Verdict.Unresolved)
      },
      test("resolved nodes in LATER sections do not reclassify a still-unresolved read") {
        // The regression this guards: the Analyzed/Optimized sections of the
        // healthy fixture are full of `SubqueryAlias`/`Relation … parquet` for
        // the same table. Only the PARSED section speaks to the question.
        val classification = PlanDiagnostics.classify(unresolvedExplain, List("silver")).head
        assertTrue(
          classification.verdict == PlanDiagnostics.Verdict.Unresolved,
          classification.evidence.contains("+- 'UnresolvedRelation [silver], [], false".trim),
        )
      },
      test("a RelationV2 in the PARSED plan reads as already resolved") {
        assertTrue(
          verdictOf(preResolvedV2Explain, "silver") ==
            PlanDiagnostics.Verdict.PreResolved("RelationV2")
        )
      },
      test("SubqueryAlias + Relation reports the RELATION, not the wrapper") {
        val classification = PlanDiagnostics.classify(preResolvedV1Explain, List("silver")).head
        assertTrue(
          classification.verdict == PlanDiagnostics.Verdict.PreResolved("Relation"),
          classification.evidence.exists(_.contains("parquet")),
        )
      },
      test("a multipart name is recognised in its bracketed spelling") {
        assertTrue(
          verdictOf(multipartExplain, "bronze.orders") == PlanDiagnostics.Verdict.Unresolved
        )
      },
      test("a read the plan never mentions is NOT classified — the heuristic declines") {
        assertTrue(verdictOf(unresolvedExplain, "gold") == PlanDiagnostics.Verdict.NotFound)
      },
      test("matching is at identifier boundaries: 'silver' is not 'silver_raw'") {
        assertTrue(
          PlanDiagnostics.mentions("+- 'UnresolvedRelation [silver], [], false", "silver"),
          !PlanDiagnostics.mentions("+- 'UnresolvedRelation [silver_raw], [], false", "silver"),
          !PlanDiagnostics.mentions("+- 'UnresolvedRelation [my_silver], [], false", "silver"),
          // catalog/schema qualification IS a match — that is the same table
          PlanDiagnostics.mentions("+- RelationV2[id#1] warehouse.dev.silver", "silver"),
          // …but a different table under the same prefix is not
          !PlanDiagnostics.mentions("+- RelationV2[id#1] warehouse.silver.orders", "silver"),
        )
      },
      test("node types survive the tree drawing characters") {
        assertTrue(
          PlanDiagnostics.nodeType("+- 'UnresolvedRelation [x], [], false") == "'UnresolvedRelation",
          PlanDiagnostics.nodeType("   +- Relation spark_catalog.default.x[id#1] parquet") == "Relation",
          PlanDiagnostics.nodeType(":- RelationV2[id#1] cat.db.x") == "RelationV2",
          PlanDiagnostics.nodeType("Project [id#1]") == "Project",
        )
      },
      test("several reads are classified independently") {
        val mixed =
          """== Parsed Logical Plan ==
            |'Join Inner
            |:- 'UnresolvedRelation [bronze], [], false
            |+- RelationV2[id#3] warehouse.dev.silver
            |""".stripMargin
        val verdicts = PlanDiagnostics.classify(mixed, List("bronze", "silver")).map(_.verdict)
        assertTrue(
          verdicts == List(
            PlanDiagnostics.Verdict.Unresolved,
            PlanDiagnostics.Verdict.PreResolved("RelationV2"),
          )
        )
      },
    ),
    suite("rendering — factual, never prescriptive")(
      test("each line states what the parsed plan SHOWS, and nothing more") {
        val unresolved = PlanDiagnostics
          .ReadClassification("silver", PlanDiagnostics.Verdict.Unresolved, None)
          .render
        val resolved = PlanDiagnostics
          .ReadClassification("silver", PlanDiagnostics.Verdict.PreResolved("RelationV2"), None)
          .render
        val unknown = PlanDiagnostics
          .ReadClassification("silver", PlanDiagnostics.Verdict.NotFound, None)
          .render
        assertTrue(
          unresolved == "read 'silver': UnresolvedRelation in Parsed plan",
          resolved == "read 'silver': already resolved in Parsed plan (RelationV2)",
          unknown.contains("not found in Parsed plan"),
          unknown.contains("heuristic"),
        )
      },
      test("no line recommends a code change — this tool reports, it does not advise") {
        // The project's standing decision: the eager-analysis behaviour is an
        // UPSTREAM bug we file and wait on, not something users are told to
        // code around. A diagnostic that grows advice grows a position.
        val lines = List(
          PlanDiagnostics.Verdict.Unresolved,
          PlanDiagnostics.Verdict.PreResolved("RelationV2"),
          PlanDiagnostics.Verdict.NotFound,
        ).map(v => PlanDiagnostics.ReadClassification("silver", v, None).render)
        val banned = List("prefer", "instead", "AT RISK", "should", "avoid", "withColumn")
        assertTrue(lines.forall(line => banned.forall(!line.contains(_))))
      },
      test("the one accompanying note states upstream behaviour as fact, and points at the issue") {
        assertTrue(
          PlanDiagnostics.PreResolvedNote.contains("not registered as a pipeline dependency"),
          PlanDiagnostics.PreResolvedNote.contains("Spark 4.2.x"),
          PlanDiagnostics.PreResolvedNote.contains("docs/plugin.md"),
        )
      },
    ),
  )
