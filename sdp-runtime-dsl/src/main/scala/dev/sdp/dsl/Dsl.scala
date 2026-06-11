package dev.sdp.dsl

import dev.sdp.core.GraphFragment

/** Author-facing DSL for declaring Spark Declarative Pipelines datasets.
  *
  * These types are deliberately Spark-free: the macro layer only inspects the
  * *shape* of the code (method names, literal arguments), never executes it.
  * The Spark Connect adapter gives them real semantics later, in the
  * Infrastructure ring.
  */

/** Opaque handle to a dataset reference inside a pipeline body. */
final class DataFrameRef private[dsl] ():
  def join(other: DataFrameRef): DataFrameRef = this

/** Mirror of Spark's `DataStreamReader` surface — shape only. The macro reads
  * these method names from the AST; the bodies never run during extraction.
  */
final class StreamReader private[dsl] ():
  def table(@annotation.unused name: String): DataFrameRef = DataFrameRef()

/** The context value threaded through pipeline bodies. Mirrors the slice of
  * `SparkSession` the macro understands.
  */
final class PipelineContext private[dsl] ():
  def readStream: StreamReader = StreamReader()
  def table(@annotation.unused name: String): DataFrameRef = DataFrameRef()

/** Declare a batch table dataset. The name must be a string literal — it is
  * extracted at compile time into the pipeline graph.
  *
  * {{{
  * val orders = table("bronze_orders")
  * }}}
  */
transparent inline def table(inline name: String): GraphFragment =
  ${ DslMacros.tableImpl('name) }

/** Declare an **external / source table** the pipeline reads but does not own —
  * it already exists in the catalog (a bronze layer, a shared dimension, …).
  *
  * {{{
  * val orders = externalTable("main.bronze.orders")   // exists in Unity Catalog
  * val gold = materializedView("gold") {
  *   spark.table("main.bronze.orders").select(col("id"))   // resolves to the catalog read
  * }
  * }}}
  *
  * Reads of it resolve at build time (no dangling-edge error), and the server
  * resolves it from the catalog at run time — it is never registered as a
  * pipeline-managed dataset. Typos of *managed* dataset names still fail: only
  * names you explicitly declare external are treated as external. The name
  * must be a string literal. */
transparent inline def externalTable(inline name: String): GraphFragment =
  ${ DslMacros.externalTableImpl('name) }

/** Declare a streaming table whose body reads from upstream datasets. Every
  * `table("...")` reference inside the body becomes a lineage edge into this
  * dataset, extracted at compile time — the body is never executed during
  * extraction.
  *
  * @deprecated The ctx-style body captures lineage *shape only* — it carries
  *             no transformation, and shape-only table feeds fail server-side
  *             resolution. Use [[streamingTableFrom]] (typed flow language)
  *             or [[sqlStreamingTable]] (SQL body) instead; both carry real
  *             flow definitions.
  */
@deprecated(
  "shape-only bodies fail server-side resolution; use streamingTableFrom or sqlStreamingTable",
  "0.1.0",
)
transparent inline def streamingTable(inline name: String)(
    inline body: PipelineContext => DataFrameRef
): GraphFragment =
  ${ DslMacros.streamingTableImpl('name, 'body) }

/** Declare a materialized view: a batch dataset precomputed from exactly one
  * SQL transformation. Both the name and the SQL must be string literals.
  *
  * {{{
  * val daily = materializedView("daily_totals")("SELECT day, sum(x) FROM silver_orders GROUP BY day")
  * }}}
  */
transparent inline def materializedView(inline name: String)(inline sql: String): GraphFragment =
  ${ DslMacros.materializedViewImpl('name, 'sql) }

/** Declare a temporary view: an ephemeral dataset scoped to one pipeline
  * run, defined by a SQL query. Both arguments must be string literals.
  *
  * {{{
  * val base = temporaryView("base_numbers")("SELECT id FROM range(1, 100)")
  * }}}
  */
transparent inline def temporaryView(inline name: String)(inline sql: String): GraphFragment =
  ${ DslMacros.temporaryViewImpl('name, 'sql) }

/** Declare a (streaming) table whose defining flow is a SQL query. On the
  * server every table is a streaming table — the query must produce a
  * streaming relation, e.g. by reading upstream datasets with `STREAM(...)`:
  *
  * {{{
  * val silver = sqlStreamingTable("silver_orders")(
  *   "SELECT * FROM STREAM(bronze_orders) WHERE amount > 0"
  * )
  * }}}
  */
transparent inline def sqlStreamingTable(inline name: String)(inline sql: String): GraphFragment =
  ${ DslMacros.sqlStreamingTableImpl('name, 'sql) }

/** Declare a streaming table whose flow is written in the typed flow
  * language — extracted to a relation tree at compile time, validated
  * locally (lineage, cycles) and remotely (full analysis on dry-run):
  *
  * {{{
  * val gold = streamingTableFrom("gold_rates") {
  *   val base = stream.table("silver_rates").where(col("value") > lit(0))
  *   base.select(col("value"), (col("value") * lit(2)).as("doubled"))
  * }
  * }}}
  */
transparent inline def streamingTableFrom(inline name: String)(inline body: FlowRel): GraphFragment =
  ${ DslMacros.streamingTableFromImpl('name, 'body) }

/** Declare a materialized view whose (batch) flow is written in the typed
  * flow language:
  *
  * {{{
  * val daily = materializedViewFrom("daily_counts") {
  *   read.table("gold_rates").groupBy(col("value")).agg(fn("count", col("value")).as("n"))
  * }
  * }}}
  */
transparent inline def materializedViewFrom(inline name: String)(inline body: FlowRel): GraphFragment =
  ${ DslMacros.materializedViewFromImpl('name, 'body) }

/** Spark-faithful spellings: a flow-language body distinguishes these from
  * the SQL-string overloads (`materializedView(name)("SELECT ...")`) by type,
  * so the dataset-kind verb reads exactly as in the SDP guide:
  *
  * {{{
  * val gold = streamingTable("gold_rates") {
  *   spark.readStream.table("silver_rates").where(col("value") > lit(0))
  * }
  * val daily = materializedView("daily") {
  *   spark.table("orders").groupBy("state").count()
  * }
  * }}}
  */
transparent inline def streamingTable(inline name: String)(inline body: FlowRel): GraphFragment =
  ${ DslMacros.streamingTableFromImpl('name, 'body) }

transparent inline def materializedView(inline name: String)(inline body: FlowRel): GraphFragment =
  ${ DslMacros.materializedViewFromImpl('name, 'body) }
