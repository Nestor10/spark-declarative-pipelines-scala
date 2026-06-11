package dev.sdp.dsl.runtime

import dev.sdp.core.algebra.*
import dev.sdp.core.{Flow, GraphFragment, PipelineNode}

/** D10 / M1: a *runtime* plan-builder that is THE frontend — producing the
  * exact same `Rel`/`Ex` algebra trees the compile-time `FlowExtractor` macro
  * produces, but by ordinary value-level method calls rather than AST
  * reflection.
  *
  * The builder is split across this package:
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
  * Contrast with the macro frontend:
  *   - macro: `transparent inline def` entry points + `${ … }` splice +
  *     `quotes.reflect` pattern matching over `Term`s, TASTy embedding.
  *   - runtime: plain `def`s, builder values, no inline, no TASTy. The body
  *     simply *runs* and the last `Df` carries the finished `Rel`.
  *
  * Each builder method's doc cites the `FlowExtractor` case it mirrors.
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

/** An explicit, unordered collection of fragments — the runtime analogue of
  * the sbt task gathering macro-emitted fragments from every compilation unit.
  * Feeds `ManifestAssembly` unchanged. */
object Pipeline:
  def apply(fragments: GraphFragment*): List[GraphFragment] = fragments.toList
