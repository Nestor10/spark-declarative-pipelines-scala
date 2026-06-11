package dev.sdp.dsl

import dev.sdp.core.algebra.*
import dev.sdp.core.{Flow, GraphFragment, PipelineNode}

/** D10: a *runtime* plan-builder that IS the DSL frontend — producing
  * `Rel`/`Ex` algebra trees by ordinary value-level method calls. (Design
  * history: this replaced a compile-time macro frontend — `transparent inline`
  * entry points + `quotes.reflect` AST extraction + TASTy embedding. D10 cut
  * that over to the runtime builder; the macro frontend is gone.)
  *
  * The builder is split across the `dev.sdp.dsl` package:
  *   - [[Column]] (`Column.scala`) — the expression surface (`ExArg` analogue).
  *   - `Expressions.scala` — top-level `col`/`lit`/`star`/`expr`/`fn`/`lam`/
  *     `lam2`/`exists`/`scalar` and the schema-type tokens.
  *   - [[functions]] (`Functions.scala`) — the full Spark `functions` facade.
  *   - [[Window]]/[[WindowSpecB]] (`Window.scala`) — window specs.
  *   - [[Df]]/[[GroupedDf]]/[[NaFunctionsB]]/[[StatFunctionsB]]
  *     (`Relations.scala`) — the relation surface (`FlowRel` analogue).
  *   - [[spark]]/[[stream]]/[[read]]/[[SourceB]] (`Sources.scala`) — readers.
  *   - [[InlineRows]]/[[InlineDf]]/[[LocalDataBuilder]] (`InlineData.scala`) —
  *     inline literal tables.
  *
  * Authors import one package — `dev.sdp.dsl.*` (+ `dev.sdp.dsl.functions.*`)
  * — and write plain `def`s and builder values: the body simply *runs* and the
  * last `Df` carries the finished `Rel`. The whole host language is available
  * inside a body (helper functions, loops, conditionals) — none of the
  * literal-argument restrictions the macro frontend imposed.
  */

/** An `InlineDf` (createDataFrame without `.toDF`) is usable as a relation,
  * columns defaulting to `_1.._N` — matching the macro's bare-`createDataFrame`
  * case (FlowExtractor.scala line 653). */
given Conversion[InlineDf, Df] = _.asDf

// ====================================================================
// entry points — plain functions (no inline, no TASTy)
// ====================================================================

/** Default storage format for tables — matches `DslMacros.DefaultFormat`
  * ("delta"). */
private val DefaultFormat = "delta"

/** External / source table the pipeline reads but does not own. Mirrors
  * `DslMacros.externalTableImpl`: a single `ExternalTable` node, no flow. */
def externalTable(name: String): GraphFragment =
  GraphFragment(List(PipelineNode.ExternalTable(name)), Set.empty)

/** A pipeline-managed batch table, *declared* (no defining flow) — a single
  * `Table` node with the default format. Mirrors the macro's no-body `table`
  * entry point; the table's data is produced by a flow declared elsewhere (or
  * seeded), so it carries no flow itself. */
def table(name: String): GraphFragment =
  GraphFragment(List(PipelineNode.Table(name, DefaultFormat)), Set.empty)

/** A pipeline-managed batch table backed by a flow body — a `Table` node plus
  * the flow that defines it (format = "delta"). Mirrors the macro's `table`
  * entry point with a body; the batch analogue of [[streamingTable]]. */
def table(name: String)(body: => Df): GraphFragment =
  GraphFragment(
    List(PipelineNode.Table(name, DefaultFormat)),
    Set.empty,
    List(Flow(name, name, body.rel)),
  )

/** Materialized view backed by a flow body — the flow IS the definition; the
  * node's sql slot is empty (matches `materializedViewFromImpl`). */
def materializedView(name: String)(body: => Df): GraphFragment =
  GraphFragment(
    List(PipelineNode.MaterializedView(name, "")),
    Set.empty,
    List(Flow(name, name, body.rel)),
  )

/** Materialized view backed by a flow body — Spark-faithful `...From` spelling
  * (matches `materializedViewFromImpl`). Identical to [[materializedView]]; both
  * names exist for muscle memory, as in `Dsl.scala`. */
def materializedViewFrom(name: String)(body: => Df): GraphFragment =
  materializedView(name)(body)

/** Streaming table backed by a flow body (matches `streamingTableFromImpl`,
  * format = "delta"). */
def streamingTable(name: String)(body: => Df): GraphFragment =
  GraphFragment(
    List(PipelineNode.StreamingTable(name, DefaultFormat)),
    Set.empty,
    List(Flow(name, name, body.rel)),
  )

/** Streaming table backed by a flow body — Spark-faithful `...From` spelling
  * (matches `streamingTableFromImpl`). Identical to [[streamingTable]]. */
def streamingTableFrom(name: String)(body: => Df): GraphFragment =
  streamingTable(name)(body)

/** Materialized view defined by a SQL string (matches `materializedViewImpl`):
  * the SQL lands as node metadata, no flow. Spark-faithful curried spelling,
  * distinguished from the flow-body overload by the second param's type
  * (`String` vs `=> Df`), exactly as `Dsl.scala` distinguishes them. */
def materializedView(name: String)(sql: String): GraphFragment =
  GraphFragment(List(PipelineNode.MaterializedView(name, sql)), Set.empty)

/** Temporary view defined by a SQL string (matches `temporaryViewImpl`). */
def temporaryView(name: String)(sql: String): GraphFragment =
  GraphFragment(List(PipelineNode.TemporaryView(name, sql)), Set.empty)

/** Streaming table whose defining flow is a SQL query (matches
  * `sqlStreamingTableImpl`): a table node plus an authored SQL flow targeting
  * it — the SQL is the flow body, not node metadata. */
def sqlStreamingTable(name: String)(sql: String): GraphFragment =
  GraphFragment(
    List(PipelineNode.StreamingTable(name, DefaultFormat)),
    Set.empty,
    List(Flow(name, name, Rel.Sql(sql))),
  )

// ====================================================================
// Pipeline — explicit, unordered aggregation (the ZIO-provide ergonomic)
// ====================================================================

/** An explicit, unordered collection of fragments — the author lists every
  * `table`/`streamingTable`/`view` value that makes up the pipeline. Feeds
  * `ManifestAssembly` (and `SdpApp.pipeline`) unchanged. */
object Pipeline:
  def apply(fragments: GraphFragment*): List[GraphFragment] = fragments.toList
