package dev.sdp.dsl

import dev.sdp.core.algebra.*

/** The `SparkSession` reader facade (D8 — `spark.table`, `spark.read`,
  * `spark.readStream`, `spark.range`, `spark.sql` spelled exactly as in Spark).
  * `spark` is an ordinary object; each leaf returns a `Df`/`SourceB` carrying
  * the algebra leaf it builds.
  */
object spark:
  /** `spark.table(name)`. */
  def table(name: String): Df = Df(Rel.NamedTable(name, streaming = false))
  /** `spark.sql(query)`. */
  def sql(query: String): Df = Df(Rel.Sql(query))
  /** `spark.range(end)` — Spark's single-arg form, i.e. `0 until end`. */
  def range(end: Long): Df = Df(Rel.Range(0L, end, 1L))
  def range(start: Long, end: Long): Df = Df(Rel.Range(start, end, 1L))
  def range(start: Long, end: Long, step: Long): Df = Df(Rel.Range(start, end, step))

  val read: DataFrameReaderB       = DataFrameReaderB(streaming = false)
  val readStream: DataFrameReaderB = DataFrameReaderB(streaming = true)

  /** Inline literal table. Generic, eager
    * analogue: the rows are extracted via the [[InlineRows]] derivation. */
  def createDataFrame[A](data: Seq[A])(using rows: InlineRows[A]): InlineDf =
    InlineDf(data.map(rows.row).toList)

/** A reader chain shared by `spark.read` (batch) and `spark.readStream`. */
final case class DataFrameReaderB(streaming: Boolean):
  /** `read.table` / `readStream.table`. */
  def table(name: String): Df = Df(Rel.NamedTable(name, streaming))
  /** `read.format` / `readStream.format` → `DataSource` leaf. */
  def format(source: String): SourceB =
    SourceB(Rel.DataSource(source, Map.empty, streaming))

/** A `format(...)` reader chain accumulating options/schema before `load()`.
  * A bare `format(...)` is already a relation, so options/schema chain onto the
  * `DataSource` and `select`/`where` work directly. */
final case class SourceB(ds: Rel.DataSource):
  /** `.option(k, v)`. */
  def option(key: String, value: String): SourceB =
    SourceB(ds.copy(options = ds.options + (key -> value)))

  /** `.schema(ddl)` — parsed by `DdlSchema.parse`, which fills both the checked
    * `schema` and the verbatim `schemaDdl` sent on the wire. */
  def schema(ddl: String): SourceB =
    DdlSchema.parse(ddl) match
      case Right(fields) => SourceB(ds.copy(schema = fields, schemaDdl = Some(ddl)))
      case Left(msg)     => throw new IllegalArgumentException(s"invalid schema DDL: $msg")

  /** `.withSchema(field(...)*)` — renders the wire DDL from the field tokens. */
  def withSchema(fields: SchemaField*): SourceB =
    val parsed = fields.map(f => f.name -> f.colType).toList
    SourceB(ds.copy(schema = parsed, schemaDdl = Some(DdlSchema.render(parsed))))

  /** `.load()` / `.load(path)`. */
  def load(): Df             = Df(ds)
  def load(path: String): Df = Df(ds.copy(options = ds.options + ("path" -> path)))

  // `format(...)` is itself a relation, so select/where read directly off the
  // DataSource leaf.
  def select(columns: Column*): Df = Df(ds).select(columns*)
  def where(condition: Column): Df = Df(ds).where(condition)

/** Streaming reads, pre-facade spelling. */
object stream:
  def table(name: String): Df = Df(Rel.NamedTable(name, streaming = true))
  def source(format: String): SourceB =
    SourceB(Rel.DataSource(format, Map.empty, streaming = true))

/** Batch reads, pre-facade spelling. `read.range` requires two args (unlike
  * `spark.range`, which also takes the single-arg form). */
object read:
  def table(name: String): Df = Df(Rel.NamedTable(name, streaming = false))
  def range(start: Long, end: Long): Df = Df(Rel.Range(start, end, 1L))
  def range(start: Long, end: Long, step: Long): Df = Df(Rel.Range(start, end, step))
