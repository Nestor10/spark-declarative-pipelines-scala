package dev.sdp.connect

/** **What the Parsed Logical Plan says about a flow's in-graph reads**, as a
  * pure function over an `EXPLAIN EXTENDED` string.
  *
  * A Spark Declarative Pipelines flow declares a dependency by *reading* an
  * in-graph dataset: the server sees an `UnresolvedRelation` in the flow's
  * plan, recognises the name as another dataset in the same dataflow graph, and
  * registers the edge. A read that arrives already bound to a table carries no
  * name to recognise, and Spark 4.2.x does not register a pipeline dependency
  * for it (upstream issue — see `docs/plugin.md`).
  *
  * The `== Parsed Logical Plan ==` section is where the difference is legible,
  * because it is the planner's raw decode of our bytes, before the analyzer
  * touches anything:
  *
  * {{{
  * == Parsed Logical Plan ==
  * 'Project [*]
  * +- 'UnresolvedRelation [silver], [], false      <- arrives as a name
  *
  * == Parsed Logical Plan ==
  * Project [id#12]
  * +- RelationV2[id#12] warehouse.dev.silver       <- arrives already resolved
  * }}}
  *
  * Everything here is a **heuristic over rendered plan text** and is labelled as
  * such wherever it is printed. Spark's plan rendering is not an API; the
  * classification is deliberately conservative (a read it cannot find is
  * reported as not found, never as either verdict), and the plan itself is
  * always printed above the verdict so a reader can check it. This object
  * REPORTS; it does not advise.
  */
object PlanDiagnostics:

  /** The one neutral sentence that accompanies a pre-resolved read: what Spark
    * does with it, stated as fact, plus where the upstream issue is recorded.
    * No recommendation — the author decides what, if anything, to do. */
  val PreResolvedNote: String =
    "a read that arrives pre-resolved is not registered as a pipeline dependency by Spark 4.2.x " +
      "— see the upstream issue linked in docs/plugin.md"

  /** What the Parsed Logical Plan says about one in-graph read. */
  enum Verdict:
    /** The read is still a name in the parsed plan. */
    case Unresolved

    /** The read is already bound to a table in the parsed plan (`RelationV2`,
      * `Relation … parquet`, …). */
    case PreResolved(nodeType: String)

    /** No relation node mentioning this dataset was found in the section. Not
      * a verdict: the heuristic declining to guess (the read may be inside a
      * SQL body, a subquery rendered differently, or the section may be
      * missing because a non-EXTENDED mode was asked for). */
    case NotFound

  /** One read's classification, with the plan line it was read off so the
    * author can check the heuristic's work. */
  final case class ReadClassification(
      dataset: String,
      verdict: Verdict,
      evidence: Option[String],
  ):
    /** The one line printed per read: what the parsed plan SHOWS, and nothing
      * else. No verdict on the author's code and no recommendation — the plan
      * is printed directly above it, and the reader draws the conclusion. */
    def render: String = verdict match
      case Verdict.Unresolved =>
        s"read '$dataset': UnresolvedRelation in Parsed plan"
      case Verdict.PreResolved(nodeType) =>
        s"read '$dataset': already resolved in Parsed plan ($nodeType)"
      case Verdict.NotFound =>
        s"read '$dataset': not found in Parsed plan — not classified (heuristic)"

  /** The section header EXTENDED emits before the raw decode. */
  private val ParsedHeader = "== Parsed Logical Plan =="

  /** Any `== … ==` section header, which is how the parsed section ends. */
  private val SectionHeader = """^==\s.*\s==$""".r

  /** Node types that mean "this read is already bound to a table". Ordered by
    * how specific they are: the first match wins, and `SubqueryAlias` is last
    * because it is a WRAPPER — it carries the name but the relation underneath
    * it is the better answer. */
  private val ResolvedNodeTypes: List[String] =
    List(
      "RelationV2",
      "DataSourceV2Relation",
      "StreamingRelationV2",
      "StreamingRelation",
      "LogicalRelation",
      "HiveTableRelation",
      "Relation",
      "View",
      "SubqueryAlias",
    )

  private val UnresolvedNodeTypes: List[String] =
    List("'UnresolvedRelation", "UnresolvedRelation", "'UnresolvedCatalogRelation")

  /** The `== Parsed Logical Plan ==` section's body, or `None` when the string
    * has no such section (a non-EXTENDED mode, or an error string). */
  def parsedLogicalPlan(explain: String): Option[String] =
    val lines = explain.linesIterator.toList
    lines.indexWhere(_.trim == ParsedHeader) match
      case -1 => None
      case at =>
        val rest = lines.drop(at + 1)
        val end  = rest.indexWhere(l => SectionHeader.matches(l.trim))
        val body = if end == -1 then rest else rest.take(end)
        Some(body.mkString("\n"))

  /** Classify each in-graph read against the Parsed Logical Plan section.
    *
    * Conservative by construction: a read is only classified when a line that
    * BOTH names it AND is a relation node is found. Unresolved wins over
    * pre-resolved if both appear (a plan that still contains the name has not
    * lost it), which keeps a false alarm harder to produce than a miss.
    */
  def classify(explain: String, reads: List[String]): List[ReadClassification] =
    parsedLogicalPlan(explain) match
      case None => reads.map(ReadClassification(_, Verdict.NotFound, None))
      case Some(section) =>
        val lines = section.linesIterator.toList
        reads.map(read => classifyOne(lines, read))

  private def classifyOne(lines: List[String], read: String): ReadClassification =
    val matching = lines.zipWithIndex.filter((line, _) => mentions(line, read))

    val unresolved = matching.collectFirst {
      case (line, _) if UnresolvedNodeTypes.contains(nodeType(line)) => line.trim
    }

    unresolved match
      case Some(line) => ReadClassification(read, Verdict.Unresolved, Some(line))
      case None =>
        val resolved = matching.collectFirst {
          case (line, i) if ResolvedNodeTypes.contains(nodeType(line)) =>
            // A SubqueryAlias carries the NAME but not the interesting node.
            // The relation right underneath it is what the reader wants to see,
            // so look one or two lines down for a non-wrapper relation node.
            val better =
              if nodeType(line) == "SubqueryAlias" then
                lines.slice(i + 1, i + 3).find(l => ResolvedNodeTypes.contains(nodeType(l)) && nodeType(l) != "SubqueryAlias")
              else None
            val chosen = better.getOrElse(line)
            ReadClassification(read, Verdict.PreResolved(nodeType(chosen)), Some(chosen.trim))
        }
        resolved.getOrElse(ReadClassification(read, Verdict.NotFound, None))

  /** The node type at the head of a plan line: strip the tree drawing
    * (`+-`, `:-`, `:`, `|`, spaces) and take the leading identifier, keeping a
    * leading `'` because that apostrophe IS the "unresolved" marker. */
  private[connect] def nodeType(line: String): String =
    val stripped = line.dropWhile(c => c == ' ' || c == '+' || c == '-' || c == ':' || c == '|')
    val head     = stripped.takeWhile(c => c.isLetterOrDigit || c == '\'')
    head

  /** Does this plan line name `dataset`?
    *
    * Two spellings matter: the dotted one a resolved relation prints
    * (`warehouse.dev.silver`) and the bracketed multipart identifier an
    * unresolved one prints (`'UnresolvedRelation [bronze, orders]`). Matches
    * must be at identifier boundaries — a preceding `.` is allowed (that is
    * catalog/schema qualification), a preceding or trailing letter/digit/`_` is
    * not, so `silver` never matches `silver_raw`.
    */
  private[connect] def mentions(line: String, dataset: String): Boolean =
    val forms = List(dataset, dataset.split('.').mkString(", "))
    forms.exists(form => occursAtBoundary(line, form))

  private def occursAtBoundary(line: String, form: String): Boolean =
    if form.isEmpty then false
    else
      def isWordChar(c: Char) = c.isLetterOrDigit || c == '_'
      @annotation.tailrec
      def search(from: Int): Boolean =
        line.indexOf(form, from) match
          case -1 => false
          case at =>
            val beforeOk = at == 0 || !isWordChar(line.charAt(at - 1))
            val after    = at + form.length
            val afterOk =
              after >= line.length || (!isWordChar(line.charAt(after)) && line.charAt(after) != '.')
            if beforeOk && afterOk then true else search(at + 1)
      search(0)

