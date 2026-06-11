package dev.sdp.dsl

/** The fluent flow-body language: shape-only types whose method *names* are
  * the compile-time language `FlowExtractor` interprets into `Rel`/`Ex`
  * trees. Bodies never execute — every method body here is a stub.
  *
  * Designed for Spark muscle memory:
  * {{{
  * streamingTableFrom("gold") {
  *   val base = stream.table("silver").where(col("value") > lit(0))
  *   base.select(col("id"), (col("value") * lit(2)).as("doubled"))
  * }
  * }}}
  *
  * The language is deliberately bounded: what the extractor doesn't
  * recognize is a compile error with a position, never a silent drop.
  */

/** A relation under construction. */
final class FlowRel private[dsl] ():
  def select(columns: ExArg*): FlowRel               = this
  def select(col1: String, cols: String*): FlowRel   = this
  def where(condition: ExArg): FlowRel               = this
  def filter(condition: ExArg): FlowRel              = this // Spark alias for where
  def groupBy(columns: ExArg*): GroupedRel           = GroupedRel()
  def groupBy(col1: String, cols: String*): GroupedRel = GroupedRel()
  def join(right: FlowRel)(on: ExArg): FlowRel       = this
  def joinLeft(right: FlowRel)(on: ExArg): FlowRel   = this
  def joinRight(right: FlowRel)(on: ExArg): FlowRel  = this
  def joinFull(right: FlowRel)(on: ExArg): FlowRel   = this
  def joinSemi(right: FlowRel)(on: ExArg): FlowRel   = this
  def joinAnti(right: FlowRel)(on: ExArg): FlowRel   = this
  def crossJoin(right: FlowRel): FlowRel             = this
  def orderBy(keys: ExArg*): FlowRel                 = this
  def orderBy(col1: String, cols: String*): FlowRel  = this
  def sort(keys: ExArg*): FlowRel                    = this // Spark alias for orderBy
  def sort(col1: String, cols: String*): FlowRel     = this
  def limit(n: Int): FlowRel                         = this
  def offset(n: Int): FlowRel                        = this
  def tail(n: Int): FlowRel                          = this
  def distinct: FlowRel                              = this
  def dropDuplicates(columns: String*): FlowRel      = this
  def drop(columns: String*): FlowRel                = this
  def union(other: FlowRel): FlowRel                 = this
  def intersect(other: FlowRel): FlowRel             = this
  def except(other: FlowRel): FlowRel                = this
  def intersectAll(other: FlowRel): FlowRel          = this
  def exceptAll(other: FlowRel): FlowRel             = this
  def alias(name: String): FlowRel                   = this
  def toDF(columnNames: String*): FlowRel            = this
  def withColumn(colName: String, col: ExArg): FlowRel = this

  /** Spark's exact signature: `withColumns(colsMap: Map[String, Column])`. */
  def withColumns(colsMap: Map[String, ExArg]): FlowRel = this

  def withColumnRenamed(existingName: String, newName: String): FlowRel = this

  /** Spark's exact signatures: a `Map` of renames, or parallel name lists. */
  def withColumnsRenamed(colsMap: Map[String, String]): FlowRel = this
  def withColumnsRenamed(colNames: Seq[String], newColNames: Seq[String]): FlowRel = this
  def sample(fraction: Double): FlowRel              = this
  def sample(fraction: Double, seed: Long): FlowRel  = this
  def hint(name: String, parameters: ExArg*): FlowRel = this
  def repartition(n: Int): FlowRel                   = this
  def coalesce(n: Int): FlowRel                      = this
  def repartitionBy(exprs: ExArg*): FlowRel          = this
  def dropNa(columns: String*): FlowRel              = this
  def fillNa(value: ExArg, columns: String*): FlowRel = this
  def describe(columns: String*): FlowRel            = this
  def summary(statistics: String*): FlowRel          = this
  def crosstab(col1: String, col2: String): FlowRel  = this
  def cov(col1: String, col2: String): FlowRel       = this
  def corr(col1: String, col2: String): FlowRel      = this
  def freqItems(columns: String*): FlowRel           = this
  def unpivot(ids: ExArg*)(variableColumnName: String, valueColumnName: String): FlowRel = this
  def transpose(indexColumns: ExArg*): FlowRel       = this
  def replaceValues(oldValue: ExArg, newValue: ExArg, columns: String*): FlowRel = this
  // sampleBy / approxQuantile: programmatic (Rel.SampleBy / Rel.ApproxQuantile)
  // only — their argument shapes (tuple varargs, triple groups) fight
  // extraction for little author value.
  def observe(name: String, metrics: ExArg*): FlowRel = this

  /** `df.na` — null-handling namespace, Spark style. */
  def na: NaFunctions = NaFunctions()

  /** `df.stat` — statistics namespace, Spark style. */
  def stat: StatFunctions = StatFunctions()

/** `df.na.*` — `DataFrameNaFunctions` mirror. */
final class NaFunctions private[dsl] ():
  def drop(): FlowRel                              = FlowRel()
  def drop(cols: Seq[String]): FlowRel             = FlowRel()
  def fill(value: ExArg): FlowRel                  = FlowRel()
  def fill(value: ExArg, cols: Seq[String]): FlowRel = FlowRel()
  def replace(col: String, replacement: Map[ExArg, ExArg]): FlowRel       = FlowRel()
  def replace(cols: Seq[String], replacement: Map[ExArg, ExArg]): FlowRel = FlowRel()

/** `df.stat.*` — `DataFrameStatFunctions` mirror. */
final class StatFunctions private[dsl] ():
  def crosstab(col1: String, col2: String): FlowRel = FlowRel()
  def cov(col1: String, col2: String): FlowRel      = FlowRel()
  def corr(col1: String, col2: String): FlowRel     = FlowRel()
  def freqItems(cols: Seq[String]): FlowRel         = FlowRel()

/** `groupBy(...)` awaiting its aggregates. */
final class GroupedRel private[dsl] ():
  def agg(aggregates: ExArg*): FlowRel = FlowRel()

  /** `groupBy(...).count()` — the Spark convenience: one `count` aggregate
    * named `count`. */
  def count(): FlowRel = FlowRel()

/** A streaming-source builder: `stream.source("rate").option(k, v)`. */
final class SourceRel private[dsl] ():
  def option(key: String, value: String): SourceRel = this

  /** Declare this source's columns — the only place schemas are *declared*;
    * everywhere else they're inferred. Downstream column typos then fail at
    * `sbt compile` listing the available columns:
    *
    * {{{
    * stream.source("rate").withSchema(field("timestamp", timestamp), field("value", long))
    * }}}
    */
  def withSchema(fields: SchemaField*): SourceRel   = this

  /** Declare this source's columns with Spark's DDL schema string — the same
    * string `DataFrameReader.schema(...)` takes. The macro parses it at
    * compile time; downstream column typos then fail at `sbt compile`:
    *
    * {{{
    * spark.readStream.format("rate").schema("timestamp TIMESTAMP, value BIGINT").load()
    * }}}
    *
    * Checked types: BOOLEAN, INT, BIGINT, DOUBLE, STRING, TIMESTAMP, DATE.
    * Other valid Spark types keep the column *name* checked, type gradual.
    */
  def schema(ddl: String): SourceRel = this

  /** Terminate a reader chain, Spark-style: `spark.read.format("csv").load()`.
    * The path variant records the path as the `path` option, as Spark does.
    */
  def load(): FlowRel            = FlowRel()
  def load(path: String): FlowRel = FlowRel()
  def select(columns: ExArg*): FlowRel              = FlowRel()
  def where(condition: ExArg): FlowRel              = FlowRel()

/** A declared source column; build with [[field]]. */
final class SchemaField private[dsl] ()

/** Column type tokens for [[SourceRel.withSchema]]. */
final class SchemaType private[dsl] ()
val bool: SchemaType      = SchemaType()
val int: SchemaType       = SchemaType()
val long: SchemaType      = SchemaType()
val double: SchemaType    = SchemaType()
val string: SchemaType    = SchemaType()
val timestamp: SchemaType = SchemaType()
val date: SchemaType      = SchemaType()

def field(name: String, tpe: SchemaType): SchemaField = SchemaField()

/** The `SparkSession` mirror — entry points shaped *exactly* like Spark, so
  * existing Spark code extracts unchanged:
  *
  * {{{
  * spark.table("customer_orders")                          // batch read
  * spark.readStream.table("orders")                        // streaming read
  * spark.readStream.format("rate").option("k", "v").load() // streaming source
  * spark.read.format("csv").load("/data/seed.csv")         // batch source
  * spark.sql("SELECT ...")                                 // SQL relation
  * spark.range(1, 100)                                     // generated rows
  * }}}
  *
  * `spark` is a phantom: there is no session and bodies never execute — the
  * names exist purely so Spark muscle memory compiles.
  */
object spark:
  def table(name: String): FlowRel = FlowRel()

  /** SQL relation — the escape hatch, Spark-spelled. */
  def sql(query: String): FlowRel = FlowRel()

  /** Overloads instead of default args: default args desugar to synthetic
    * method calls the compile-time extractor would have to special-case.
    */
  def range(end: Long): FlowRel                          = FlowRel()
  def range(start: Long, end: Long): FlowRel             = FlowRel()
  def range(start: Long, end: Long, step: Long): FlowRel = FlowRel()

  /** Inline literal table — Spark-spelled. The macro reads the literal rows
    * (and `.toDF(...)` column names) and lowers them to SQL `VALUES`; the body
    * never runs. Rows are tuples (or single values for a one-column table):
    *
    * {{{
    * spark.createDataFrame(Seq((1, "North"), (2, "South"))).toDF("id", "name")
    * }}}
    *
    * For small lookup/seed/enum tables only — a build-time guard caps the size.
    */
  def createDataFrame[A](data: Seq[A]): FlowRel = FlowRel()

  val read: DataFrameReader        = DataFrameReader()
  val readStream: DataStreamReader = DataStreamReader()

/** `spark.read` — batch reads and batch sources. */
final class DataFrameReader private[dsl] ():
  def table(name: String): FlowRel      = FlowRel()
  def format(source: String): SourceRel = SourceRel()

/** `spark.readStream` — streaming reads and streaming sources. */
final class DataStreamReader private[dsl] ():
  def table(name: String): FlowRel      = FlowRel()
  def format(source: String): SourceRel = SourceRel()

/** Streaming reads (pre-facade spelling; prefer `spark.readStream`). */
object stream:
  def table(name: String): FlowRel    = FlowRel()
  def source(format: String): SourceRel = SourceRel()

/** Batch reads (pre-facade spelling; prefer `spark.table` / `spark.read`). */
object read:
  def table(name: String): FlowRel = FlowRel()

  /** Generated rows `start until end` (by `step`) — the test/dimension leaf.
    * Overloads instead of a default arg: default args desugar to synthetic
    * method calls the compile-time extractor would have to special-case.
    */
  def range(start: Long, end: Long): FlowRel             = FlowRel()
  def range(start: Long, end: Long, step: Long): FlowRel = FlowRel()

/** An expression under construction. */
final class ExArg private[dsl] ():
  def +(other: ExArg): ExArg  = this
  def -(other: ExArg): ExArg  = this
  def *(other: ExArg): ExArg  = this
  def /(other: ExArg): ExArg  = this
  def %(other: ExArg): ExArg  = this
  def >(other: ExArg): ExArg  = this
  def <(other: ExArg): ExArg  = this
  def >=(other: ExArg): ExArg = this
  def <=(other: ExArg): ExArg = this
  def ===(other: ExArg): ExArg = this
  def =!=(other: ExArg): ExArg = this
  def &&(other: ExArg): ExArg = this
  def ||(other: ExArg): ExArg = this
  def as(name: String): ExArg = this
  def asc: ExArg              = this
  def desc: ExArg             = this

  /** ANSI cast to a schema type token: `col("n").cast(long)`. */
  def cast(to: SchemaType): ExArg = this

  /** Container access: map key / array index — `col("m").getItem(lit("k"))`. */
  def getItem(key: ExArg): ExArg = this

  /** Struct field access — `col("s").getField("inner")`. */
  def getField(name: String): ExArg = this

  /** Membership in a subquery's result: `col("id").in(read.table("ids"))`. */
  def in(sub: FlowRel): ExArg = this

  /** Membership in a literal set — Spark's `Column.isin`:
    * `col("state").isin(lit("CA"), lit("NY"))`. */
  def isin(values: ExArg*): ExArg = this

  /** Chain another CASE branch: `when(c1, v1).when(c2, v2)` — Spark style. */
  def when(condition: ExArg, value: ExArg): ExArg = this

  /** The CASE else: `when(cond, v).otherwise(other)` — Spark style. */
  def otherwise(value: ExArg): ExArg = this

  /** Apply this (aggregate/ranking) function over a window:
    * `fn("row_number").over(window.partitionBy(col("k")).orderBy(col("ts").desc))`.
    */
  def over(spec: WindowSpec): ExArg = this

/** Window specification builder — Spark style. */
final class WindowSpec private[dsl] ():
  def orderBy(keys: ExArg*): WindowSpec = this

/** `org.apache.spark.sql.expressions.Window` mirror. */
object Window:
  def partitionBy(columns: ExArg*): WindowSpec = WindowSpec()
  def orderBy(keys: ExArg*): WindowSpec        = WindowSpec()

/** Pre-facade lowercase spelling; prefer [[Window]]. A `val` alias rather than
  * a second object — two objects differing only in case collide on a
  * case-insensitive filesystem (`Window$.class` vs `window$.class`). */
val window: Window.type = Window

/** `*` — `select(star)`, `fn("count", star)`. */
val star: ExArg = ExArg()

/** SQL-fragment escape hatch inside expressions: `expr("id % 7")`. */
def expr(sql: String): ExArg = ExArg()

/** A lambda for higher-order functions — a *real* Scala lambda; your
  * parameter name becomes the lambda variable on the wire:
  *
  * {{{
  * fn("transform", col("xs"), lam(x => x + lit(1)))
  * fn("filter",    col("xs"), lam(x => x > lit(0)))
  * }}}
  */
def lam(f: ExArg => ExArg): ExArg = ExArg()

/** Two-parameter lambda (`zip_with`, `aggregate`, map functions). */
def lam2(f: (ExArg, ExArg) => ExArg): ExArg = ExArg()

/** EXISTS subquery: `where(exists(spark.table("flags").where(...)))`. A DSL
  * combinator (lives in `dev.sdp.dsl`, not `functions`) — the Spark array-HOF
  * `exists` is reachable via `fn("exists", col, lam)`. */
def exists(sub: FlowRel): ExArg = ExArg()

/** Scalar subquery: `select(scalar(read.table("stats").select(...)).as("max_v"))`. */
def scalar(sub: FlowRel): ExArg = ExArg()

/** Column reference. */
def col(name: String): ExArg = ExArg()

/** Literal value (Int, Long, Double, Boolean or String). */
def lit(value: Int): ExArg     = ExArg()
def lit(value: Long): ExArg    = ExArg()
def lit(value: Double): ExArg  = ExArg()
def lit(value: Boolean): ExArg = ExArg()
def lit(value: String): ExArg  = ExArg()

/** Any Spark SQL function by name: `fn("count", col("id"))`. */
def fn(name: String, args: ExArg*): ExArg = ExArg()
