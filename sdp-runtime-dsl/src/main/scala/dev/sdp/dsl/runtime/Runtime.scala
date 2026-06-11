package dev.sdp.dsl.runtime

import dev.sdp.core.algebra.*
import dev.sdp.core.{Flow, GraphFragment, PipelineNode}

/** SPIKE: a *runtime* plan-builder that produces the exact same `Rel`/`Ex`
  * algebra trees the compile-time `FlowExtractor` macro produces — but by
  * ordinary value-level method calls rather than AST reflection.
  *
  * Every method on a [[Df]] / [[Column]] appends a node to the algebra tree
  * (eagerly, at call time) instead of being interpreted from the typed AST.
  * The author surface is kept as close to `dev.sdp.dsl` as the language
  * allows; the only difference at a call site is the import.
  *
  * Contrast with the macro frontend:
  *   - macro: `transparent inline def` entry points + `${ … }` splice +
  *     `quotes.reflect` pattern matching over `Term`s, TASTy embedding.
  *   - runtime: plain `def`s, builder values, no inline, no TASTy. The body
  *     simply *runs* and the last `Df` carries the finished `Rel`.
  */

// ====================================================================
// expression builder
// ====================================================================

/** Wraps the algebra's expression type `Ex`. Mirrors the macro frontend's
  * `ExArg` surface for the operators/combinators the Warehouse needs. */
final case class Column(ex: Ex):
  // binary ops — same wire names as FlowExtractor.BinaryOps
  def >(other: Column): Column  = Column(Ex.Fn(">", List(ex, other.ex)))
  def <(other: Column): Column  = Column(Ex.Fn("<", List(ex, other.ex)))
  def +(other: Column): Column  = Column(Ex.Fn("+", List(ex, other.ex)))
  def -(other: Column): Column  = Column(Ex.Fn("-", List(ex, other.ex)))
  def *(other: Column): Column  = Column(Ex.Fn("*", List(ex, other.ex)))
  def %(other: Column): Column  = Column(Ex.Fn("%", List(ex, other.ex)))
  def ===(other: Column): Column = Column(Ex.Fn("==", List(ex, other.ex)))
  def =!=(other: Column): Column = Column(Ex.Fn("!=", List(ex, other.ex)))
  def &&(other: Column): Column = Column(Ex.Fn("and", List(ex, other.ex)))
  def ||(other: Column): Column = Column(Ex.Fn("or", List(ex, other.ex)))

  def as(name: String): Column = Column(Ex.Alias(ex, name))
  def asc: SortKey             = SortKey(ex, descending = false)
  def desc: SortKey           = SortKey(ex, descending = true)

  /** `fn.over(windowSpec)` — window application. */
  def over(spec: WindowSpecB): Column =
    Column(Ex.Window(ex, spec.partitionBy, spec.orderBy, None))

// Column args (Spark's `select("a")` / `groupBy("a","b")` String overloads
// and the `select(col(...))` Column overloads) are handled by explicit method
// overloads on Df/GroupedDf below — no implicit conversions, mirroring the
// macro frontend's overload pairs in FlowApi.scala.

// ====================================================================
// functions facade (only what the Warehouse fixtures need)
// ====================================================================

/** Mirror of the slice of `dev.sdp.dsl.functions` the Warehouse uses. Each
  * lowers to `Ex.Fn(wireName, args)` exactly as the macro's `FacadeFunctions`
  * table does. */
object functions:
  def upper(e: Column): Column      = Column(Ex.Fn("upper", List(e.ex)))
  def sum(e: Column): Column        = Column(Ex.Fn("sum", List(e.ex)))
  def count(e: Column): Column      = Column(Ex.Fn("count", List(e.ex)))
  def row_number(): Column          = Column(Ex.Fn("row_number", Nil))

// ====================================================================
// top-level expression constructors (mirror dev.sdp.dsl)
// ====================================================================

/** Column reference. */
def col(name: String): Column = Column(Ex.Col(name))

/** `*` — `count(star)`, `select(star)`. */
val star: Column = Column(Ex.Star(None))

// lit overloads — same literal kinds the macro recognizes
def lit(v: Int): Column     = Column(Ex.Lit(LitValue.I32(v)))
def lit(v: Long): Column    = Column(Ex.Lit(LitValue.I64(v)))
def lit(v: Double): Column  = Column(Ex.Lit(LitValue.F64(v)))
def lit(v: Boolean): Column = Column(Ex.Lit(LitValue.Bool(v)))
def lit(v: String): Column  = Column(Ex.Lit(LitValue.Str(v)))

// ====================================================================
// window builder (mirror dev.sdp.dsl.Window / WindowSpec)
// ====================================================================

final case class WindowSpecB(partitionBy: List[Ex], orderBy: List[SortKey]):
  // SortKey args come from Column#asc / Column#desc; a bare Column orders
  // ascending (Spark default), matching FlowExtractor.sortKey.
  def orderBy(keys: SortKey*): WindowSpecB = copy(orderBy = orderBy ++ keys)
  def orderBy(col1: Column, cols: Column*): WindowSpecB =
    copy(orderBy = orderBy ++ (col1 +: cols).map(c => SortKey(c.ex)))

object Window:
  def partitionBy(columns: Column*): WindowSpecB =
    WindowSpecB(columns.map(_.ex).toList, Nil)
  def orderBy(keys: SortKey*): WindowSpecB = WindowSpecB(Nil, keys.toList)

// ====================================================================
// relation builder
// ====================================================================

/** A relation under construction, wrapping the finished [[Rel]] node. Each
  * method appends one algebra node — the runtime analogue of the macro's
  * `rel(...)` recursive descent. */
final case class Df(rel: Rel):

  // select — Column overload and Spark's bare-String overload (→ Ex.Col)
  def select(columns: Column*): Df =
    Df(Rel.Project(rel, columns.map(_.ex).toList))
  def select(col1: String, cols: String*): Df =
    Df(Rel.Project(rel, (col1 +: cols).map(Ex.Col(_)).toList))

  def where(condition: Column): Df = Df(Rel.Filter(rel, condition.ex))
  def filter(condition: Column): Df = where(condition)

  /** Consecutive `withColumn`s collapse into one `WithColumns`, exactly like
    * the macro (FlowExtractor line 837–839). */
  def withColumn(colName: String, c: Column): Df = rel match
    case Rel.WithColumns(inner, cols) => Df(Rel.WithColumns(inner, cols :+ (colName -> c.ex)))
    case other                        => Df(Rel.WithColumns(other, List(colName -> c.ex)))

  /** Consecutive renames collapse, matching the macro (line 971–973). */
  def withColumnRenamed(existing: String, renamed: String): Df = rel match
    case Rel.WithColumnsRenamed(inner, renames) =>
      Df(Rel.WithColumnsRenamed(inner, renames :+ (existing -> renamed)))
    case other => Df(Rel.WithColumnsRenamed(other, List(existing -> renamed)))

  // groupBy — Column overload and Spark's bare-String overload (→ Ex.Col)
  def groupBy(columns: Column*): GroupedDf =
    GroupedDf(rel, columns.map(_.ex).toList)
  def groupBy(col1: String, cols: String*): GroupedDf =
    GroupedDf(rel, (col1 +: cols).map(Ex.Col(_)).toList)

/** `groupBy(...)` awaiting its aggregates. */
final case class GroupedDf(input: Rel, groups: List[Ex]):
  def agg(aggregates: Column*): Df =
    Df(Rel.Aggregate(input, groups, aggregates.map(_.ex).toList))

  /** `groupBy(...).count()` — Spark convenience: one `count` aggregate named
    * `count` (macro line 783). */
  def count(): Df =
    Df(Rel.Aggregate(input, groups, List(Ex.Alias(Ex.Fn("count", List(Ex.Star(None))), "count"))))

// ====================================================================
// source / reader facade — spark.*  (mirror dev.sdp.dsl.spark)
// ====================================================================

object spark:
  def table(name: String): Df = Df(Rel.NamedTable(name, streaming = false))

  val read: DataFrameReaderB        = DataFrameReaderB(streaming = false)
  val readStream: DataFrameReaderB  = DataFrameReaderB(streaming = true)

  /** Inline literal table. Generic, eager analogue of the macro's
    * `createDataFrame[A](Seq(...)).toDF(...)` → `Rel.LocalData`. We accept the
    * already-extracted rows via [[InlineRows]] (see [[createDataFrame]]). */
  def createDataFrame[A](data: Seq[A])(using rows: InlineRows[A]): InlineDf =
    InlineDf(data.map(rows.row).toList)

/** A reader chain shared by `spark.read` (batch) and `spark.readStream`. */
final case class DataFrameReaderB(streaming: Boolean):
  def table(name: String): Df = Df(Rel.NamedTable(name, streaming))
  def format(source: String): SourceB =
    SourceB(Rel.DataSource(source, Map.empty, streaming))

/** A `format(...)` reader chain accumulating options / schema before `load()`. */
final case class SourceB(ds: Rel.DataSource):
  def option(key: String, value: String): SourceB =
    SourceB(ds.copy(options = ds.options + (key -> value)))

  /** Spark DDL schema string — parsed by the SAME `DdlSchema.parse` the macro
    * uses, so the resulting `(schema, schemaDdl)` are identical (macro line
    * 701–717). */
  def schema(ddl: String): SourceB =
    DdlSchema.parse(ddl) match
      case Right(fields) => SourceB(ds.copy(schema = fields, schemaDdl = Some(ddl)))
      case Left(msg)     => throw new IllegalArgumentException(s"invalid schema DDL: $msg")

  def load(): Df             = Df(ds)
  def load(path: String): Df = Df(ds.copy(options = ds.options + ("path" -> path)))

// ====================================================================
// inline data: spark.createDataFrame(Seq(...)).toDF(names*) → Rel.LocalData
// ====================================================================

/** Captures one inline row as a list of literal cells. A typeclass so that
  * the *runtime* sees the concrete tuple values (the macro saw the tuple AST);
  * `.toDF(names)` then attaches the column names. Only the cell shapes the
  * Warehouse uses are provided (String here; extend as needed). */
trait InlineRows[A]:
  def row(a: A): List[LitValue]

object InlineRows:
  private def str(s: String): LitValue = LitValue.Str(s)
  given tuple2String: InlineRows[(String, String)] with
    def row(a: (String, String)): List[LitValue] = List(str(a._1), str(a._2))

/** Holds the extracted rows awaiting `.toDF(names*)`. Mirrors the macro folding
  * `.toDF` names into the `LocalData` schema (FlowExtractor line 644–652). */
final case class InlineDf(rows: List[List[LitValue]]):
  def toDF(names: String*): Df =
    LocalDataBuilder.build(rows, names.toList) match
      case Right(rel) => Df(rel)
      case Left(msg)  => throw new IllegalArgumentException(msg)

/** Replicates `FlowExtractor.localData`/`inferColType` at runtime: arity check,
  * names, and per-column type inference (numeric widening; null takes column
  * type; mixed non-numeric = error). */
object LocalDataBuilder:
  private val numericRank = Map(ColType.I32 -> 1, ColType.I64 -> 2, ColType.F64 -> 3)

  private def litType(v: LitValue): ColType = v match
    case LitValue.Bool(_) => ColType.Bool
    case LitValue.I32(_)  => ColType.I32
    case LitValue.I64(_)  => ColType.I64
    case LitValue.F64(_)  => ColType.F64
    case LitValue.Str(_)  => ColType.Str
    case LitValue.Null    => ColType.Unknown

  private def inferColType(cells: List[LitValue]): Either[String, ColType] =
    val kinds = cells.filterNot(_ == LitValue.Null).map(litType).distinct
    if kinds.isEmpty then Left("cannot infer the type of an all-null inline-table column")
    else if kinds.forall(numericRank.contains) then Right(kinds.maxBy(numericRank))
    else if kinds.sizeIs == 1 then Right(kinds.head)
    else Left(s"inconsistent inline-table column types: ${kinds.mkString(", ")}")

  def build(rows: List[List[LitValue]], names: List[String]): Either[String, Rel] =
    if rows.isEmpty then Left("spark.createDataFrame needs at least one row")
    else
      val width = rows.head.size
      if rows.exists(_.size != width) then
        Left("every row in spark.createDataFrame must have the same number of columns")
      else if names.size != width then
        Left(s".toDF gave ${names.size} name(s) for a $width-column inline table")
      else
        val types = (0 until width).toList.foldRight(Right(Nil): Either[String, List[ColType]]) {
          (j, acc) => for { t <- inferColType(rows.map(_(j))); rest <- acc } yield t :: rest
        }
        types.map(ts => Rel.LocalData(names.zip(ts), rows))

// ====================================================================
// entry points — plain functions (no inline, no TASTy)
// ====================================================================

/** Default storage format for streaming tables — matches the macro's
  * `DslMacros.DefaultFormat` ("delta"). */
private val DefaultFormat = "delta"

/** External / source table the pipeline reads but does not own. Mirrors
  * `DslMacros.externalTableImpl`: a single `ExternalTable` node, no flow. */
def externalTable(name: String): GraphFragment =
  GraphFragment(List(PipelineNode.ExternalTable(name)), Set.empty)

/** Materialized view: the flow body IS the definition; the node's sql slot is
  * empty (matches `materializedViewFromImpl`). */
def materializedView(name: String)(body: => Df): GraphFragment =
  GraphFragment(
    List(PipelineNode.MaterializedView(name, "")),
    Set.empty,
    List(Flow(name, name, body.rel)),
  )

/** Streaming table backed by a flow body (matches `streamingTableFromImpl`,
  * format = "delta"). */
def streamingTable(name: String)(body: => Df): GraphFragment =
  GraphFragment(
    List(PipelineNode.StreamingTable(name, DefaultFormat)),
    Set.empty,
    List(Flow(name, name, body.rel)),
  )

// ====================================================================
// Pipeline — explicit, unordered aggregation (the ZIO-provide ergonomic)
// ====================================================================

/** An explicit, unordered collection of fragments — the runtime analogue of
  * the sbt task gathering macro-emitted fragments from every compilation unit.
  * Merges them via the fragment monoid; feeds `ManifestAssembly` unchanged. */
object Pipeline:
  def apply(fragments: GraphFragment*): List[GraphFragment] = fragments.toList
