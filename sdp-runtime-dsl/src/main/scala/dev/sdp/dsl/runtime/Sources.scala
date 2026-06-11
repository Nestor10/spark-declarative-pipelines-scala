package dev.sdp.dsl.runtime

import dev.sdp.core.algebra.*

/** Runtime mirror of the `SparkSession` reader facade (FlowApi.scala lines
  * 163–230) interpreted by the leaf cases of `FlowExtractor.rel`
  * (FlowExtractor.scala lines 625–747). `spark` is a value-level object; each
  * leaf returns a `Df`/`SourceB` carrying the algebra leaf the macro produces.
  */
object spark:
  /** `spark.table(name)` (FlowExtractor lines 626–627). */
  def table(name: String): Df = Df(Rel.NamedTable(name, streaming = false))
  /** `spark.sql(query)` (lines 628–629). */
  def sql(query: String): Df = Df(Rel.Sql(query))
  /** `spark.range(end)` — Spark single-arg = `0 until end` (rangeRel
    * allowSingle, lines 630–631 + 487–503). */
  def range(end: Long): Df = Df(Rel.Range(0L, end, 1L))
  def range(start: Long, end: Long): Df = Df(Rel.Range(start, end, 1L))
  def range(start: Long, end: Long, step: Long): Df = Df(Rel.Range(start, end, step))

  val read: DataFrameReaderB       = DataFrameReaderB(streaming = false)
  val readStream: DataFrameReaderB = DataFrameReaderB(streaming = true)

  /** Inline literal table (FlowExtractor lines 644–654). Generic, eager
    * analogue: the rows are extracted via the [[InlineRows]] derivation. */
  def createDataFrame[A](data: Seq[A])(using rows: InlineRows[A]): InlineDf =
    InlineDf(data.map(rows.row).toList)

/** A reader chain shared by `spark.read` (batch) and `spark.readStream`
  * (FlowApi.scala lines 206–214). */
final case class DataFrameReaderB(streaming: Boolean):
  /** `read.table` / `readStream.table` (FlowExtractor lines 632–635). */
  def table(name: String): Df = Df(Rel.NamedTable(name, streaming))
  /** `read.format` / `readStream.format` → `DataSource` leaf (lines 636–639). */
  def format(source: String): SourceB =
    SourceB(Rel.DataSource(source, Map.empty, streaming))

/** A `format(...)` reader chain accumulating options/schema before `load()`
  * (FlowApi.scala SourceRel lines 113–146). The macro treats a bare
  * `format(...)` as a relation, so options/schema chain onto the `DataSource`
  * (FlowExtractor lines 686–747) and `select`/`where` work directly. */
final case class SourceB(ds: Rel.DataSource):
  /** `.option(k, v)` (FlowExtractor lines 686–696). */
  def option(key: String, value: String): SourceB =
    SourceB(ds.copy(options = ds.options + (key -> value)))

  /** `.schema(ddl)` — parsed by the SAME `DdlSchema.parse` the macro uses, so
    * `(schema, schemaDdl)` are identical (FlowExtractor lines 701–717). */
  def schema(ddl: String): SourceB =
    DdlSchema.parse(ddl) match
      case Right(fields) => SourceB(ds.copy(schema = fields, schemaDdl = Some(ddl)))
      case Left(msg)     => throw new IllegalArgumentException(s"invalid schema DDL: $msg")

  /** `.withSchema(field(...)*)` — renders a DDL from the field tokens for the
    * wire, exactly as the macro does (FlowExtractor lines 720–747). */
  def withSchema(fields: SchemaField*): SourceB =
    val parsed = fields.map(f => f.name -> f.colType).toList
    SourceB(ds.copy(schema = parsed, schemaDdl = Some(DdlSchema.render(parsed))))

  /** `.load()` / `.load(path)` (FlowExtractor lines 658–673). */
  def load(): Df             = Df(ds)
  def load(path: String): Df = Df(ds.copy(options = ds.options + ("path" -> path)))

  // SourceRel.select / SourceRel.where (FlowApi lines 145–146): the macro
  // treats `format(...)` itself as a relation, so these read directly off the
  // DataSource leaf.
  def select(columns: Column*): Df = Df(ds).select(columns*)
  def where(condition: Column): Df = Df(ds).where(condition)

/** Streaming reads, pre-facade spelling (FlowApi.scala lines 216–219;
  * FlowExtractor lines 676–681). */
object stream:
  def table(name: String): Df = Df(Rel.NamedTable(name, streaming = true))
  def source(format: String): SourceB =
    SourceB(Rel.DataSource(format, Map.empty, streaming = true))

/** Batch reads, pre-facade spelling (FlowApi.scala lines 221–230;
  * FlowExtractor lines 678–679, 682–683). `read.range` requires two args
  * (allowSingle = false). */
object read:
  def table(name: String): Df = Df(Rel.NamedTable(name, streaming = false))
  def range(start: Long, end: Long): Df = Df(Rel.Range(start, end, 1L))
  def range(start: Long, end: Long, step: Long): Df = Df(Rel.Range(start, end, step))
