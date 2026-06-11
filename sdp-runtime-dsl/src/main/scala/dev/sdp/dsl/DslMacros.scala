package dev.sdp.dsl

import scala.quoted.*

import dev.sdp.core.{DependencyEdge, Flow, GraphFragment, PipelineNode}
import dev.sdp.core.algebra.Rel

/** Compile-time extraction of pipeline structure from DSL call sites.
  *
  * Conventions (see context/scala3_spark_ast_parser.md):
  *   - `report.error` + sentinel return, never `errorAndAbort`: a file with
  *     several invalid declarations surfaces *all* diagnostics in one compile.
  *   - Structural traversal uses `TreeAccumulator`, not rigid nested pattern
  *     matches: the compiler inserts `Inlined`/`Block`/`Typed` wrappers
  *     unpredictably across versions, and a fold finds `table("...")` at any
  *     nesting depth.
  */
private[dsl] object DslMacros:

  /** Inlined constant from sdp-core (see [[dev.sdp.core.CoreEpoch]]): pins
    * sdp-core's release epoch into THIS module's bytecode, so helper-library
    * changes bust downstream action-cache keys and macros re-expand.
    * Without it, sbt 2's content-addressed cache can replay compiles with
    * stale embedded fragments (repro: 2026-06-06).
    */
  @annotation.unused
  private final val LinkedCoreEpoch = dev.sdp.core.CoreEpoch.value

  private val DefaultFormat = "delta"

  /** Expansion target for every successful extraction: the fragment is
    * computed *here, in the macro JVM*, rendered to the canonical line
    * format, and embedded as a single string literal inside a
    * `SdpMeta.embed(...)` call. That literal lands in the call site's
    * `.tasty` file, where the sbt plugin discovers it without classloading.
    */
  private def emit(fragment: GraphFragment)(using Quotes): Expr[GraphFragment] =
    '{ SdpMeta.embed(${ Expr(GraphFragment.render(fragment)) }) }

  /** `table("name")` — the smallest extraction: one literal, one node. */
  def tableImpl(name: Expr[String])(using Quotes): Expr[GraphFragment] =
    literal(name) match
      case Some(n) =>
        emit(GraphFragment(List(PipelineNode.Table(n, DefaultFormat)), Set.empty))
      case None =>
        import quotes.reflect.*
        report.error(
          s"Table name must be a constant string literal so it can be extracted at compile time; got: ${name.show}",
          name,
        )
        '{ GraphFragment.empty }

  /** `externalTable("catalog.db.name")` — declares a source the pipeline reads
    * but does not own. Emits an `ExternalTable` node so reads of it resolve
    * (not dangling) while the encoder leaves it out of the registered
    * datasets. */
  def externalTableImpl(name: Expr[String])(using Quotes): Expr[GraphFragment] =
    literal(name) match
      case Some(n) =>
        emit(GraphFragment(List(PipelineNode.ExternalTable(n)), Set.empty))
      case None =>
        import quotes.reflect.*
        report.error(
          s"External table name must be a constant string literal; got: ${name.show}",
          name,
        )
        '{ GraphFragment.empty }

  /** `streamingTable("name") { ctx => ... }` — extracts the declared node and
    * one lineage edge per upstream `table("...")` reference inside the body.
    */
  def streamingTableImpl(
      name: Expr[String],
      body: Expr[PipelineContext => DataFrameRef],
  )(using Quotes): Expr[GraphFragment] =
    import quotes.reflect.*

    literal(name) match
      case None =>
        report.error(
          s"Streaming table name must be a constant string literal; got: ${name.show}",
          name,
        )
        '{ GraphFragment.empty }

      case Some(target) =>
        // Depth-first fold over the body AST. State: upstream ids found so
        // far, most recent first. Wrapper nodes (Inlined, Block, Typed, ...)
        // are handled by foldOverTree's default recursion.
        val accumulator = new TreeAccumulator[List[String]]:
          def foldTree(found: List[String], tree: Tree)(owner: Symbol): List[String] =
            tree match
              case Apply(Select(_, "table"), List(arg)) =>
                arg match
                  case Literal(StringConstant(upstream)) =>
                    foldOverTree(upstream :: found, tree)(owner)
                  case _ =>
                    report.error(
                      s"Upstream table reference must be a constant string literal; got: ${arg.show}",
                      arg.pos,
                    )
                    foldOverTree(found, tree)(owner)
              case _ =>
                foldOverTree(found, tree)(owner)

        val upstreams = accumulator.foldTree(Nil, body.asTerm)(Symbol.spliceOwner).reverse.distinct

        emit(
          GraphFragment(
            List(PipelineNode.StreamingTable(target, DefaultFormat)),
            upstreams.map(up => DependencyEdge(up, target)).toSet,
          )
        )

  /** `materializedView("name")("sql")` — name and query both literal. */
  def materializedViewImpl(name: Expr[String], sql: Expr[String])(using Quotes): Expr[GraphFragment] =
    sqlBackedNode(name, sql, "Materialized view")(PipelineNode.MaterializedView(_, _))

  /** `temporaryView("name")("sql")` — name and query both literal. */
  def temporaryViewImpl(name: Expr[String], sql: Expr[String])(using Quotes): Expr[GraphFragment] =
    sqlBackedNode(name, sql, "Temporary view")(PipelineNode.TemporaryView(_, _))

  /** `sqlStreamingTable("name")("sql")` — a table node plus an authored
    * SQL flow targeting it (manifest v2). The SQL is the flow body, not node
    * metadata: tables on the wire have no SQL of their own.
    */
  def sqlStreamingTableImpl(name: Expr[String], sql: Expr[String])(using Quotes): Expr[GraphFragment] =
    import quotes.reflect.*
    (literal(name), literal(sql)) match
      case (Some(n), Some(q)) =>
        emit(
          GraphFragment(
            List(PipelineNode.StreamingTable(n, DefaultFormat)),
            Set.empty,
            List(Flow(n, n, Rel.Sql(q))),
          )
        )
      case (nameLit, sqlLit) =>
        if nameLit.isEmpty then
          report.error(s"SQL streaming table name must be a constant string literal; got: ${name.show}", name)
        if sqlLit.isEmpty then
          report.error(s"SQL streaming table SQL must be a constant string literal; got: ${sql.show}", sql)
        '{ GraphFragment.empty }

  private def sqlBackedNode(
      name: Expr[String],
      sql: Expr[String],
      what: String,
  )(make: (String, String) => PipelineNode)(using Quotes): Expr[GraphFragment] =
    import quotes.reflect.*
    (literal(name), literal(sql)) match
      case (Some(n), Some(q)) =>
        emit(GraphFragment(List(make(n, q)), Set.empty))
      case (nameLit, sqlLit) =>
        if nameLit.isEmpty then
          report.error(s"$what name must be a constant string literal; got: ${name.show}", name)
        if sqlLit.isEmpty then
          report.error(s"$what SQL must be a constant string literal; got: ${sql.show}", sql)
        '{ GraphFragment.empty }

  /** `streamingTableFrom("name") { <flow language> }` — the typed flow body
    * is interpreted at compile time into a relation tree.
    */
  def streamingTableFromImpl(name: Expr[String], body: Expr[FlowRel])(using Quotes): Expr[GraphFragment] =
    flowBackedNode(name, body, "Streaming table")(PipelineNode.StreamingTable(_, DefaultFormat))

  /** `materializedViewFrom("name") { <flow language> }` — batch flow. */
  def materializedViewFromImpl(name: Expr[String], body: Expr[FlowRel])(using Quotes): Expr[GraphFragment] =
    // The MV's SQL slot is unused when the flow is authored: the flow IS the
    // definition; the node carries identity only.
    flowBackedNode(name, body, "Materialized view")(PipelineNode.MaterializedView(_, ""))

  private def flowBackedNode(
      name: Expr[String],
      body: Expr[FlowRel],
      what: String,
  )(make: String => PipelineNode)(using Quotes): Expr[GraphFragment] =
    import quotes.reflect.*
    literal(name) match
      case None =>
        report.error(s"$what name must be a constant string literal; got: ${name.show}", name)
        '{ GraphFragment.empty }
      case Some(n) =>
        FlowExtractor.relation(body.asTerm) match
          case None =>
            // extractor already reported positioned errors
            '{ GraphFragment.empty }
          case Some(rel) =>
            emit(GraphFragment(List(make(n)), Set.empty, List(Flow(n, n, rel))))

  /** Compile-time constant extraction; None for anything dynamic. */
  private def literal(name: Expr[String])(using Quotes): Option[String] =
    name.value
