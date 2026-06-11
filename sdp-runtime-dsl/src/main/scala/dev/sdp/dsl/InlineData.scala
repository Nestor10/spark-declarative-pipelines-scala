package dev.sdp.dsl

import dev.sdp.core.algebra.*

/** Inline literal table support — runtime analogue of FlowExtractor's inline
  * data path (FlowExtractor.scala lines 506–597): `spark.createDataFrame(
  * Seq(...)).toDF(...)` → `Rel.LocalData`.
  *
  * Where the macro reads tuple/literal AST cells, the runtime sees the actual
  * values. [[InlineRows]] turns each row value into a `List[LitValue]`; the
  * cell kinds match the macro's `litCell` (lines 510–519): Int/Long/Double/
  * Boolean/String/Null (Scala `null`).
  */
trait InlineRows[A]:
  def row(a: A): List[LitValue]

object InlineRows:

  /** One literal cell by runtime inspection — mirrors `FlowExtractor.litCell`
    * (FlowExtractor.scala lines 510–519). `null` → `LitValue.Null` (whose
    * column type is taken from siblings during inference, like the macro). */
  private def litOfAny(a: Any): LitValue = a match
    case null       => LitValue.Null
    case v: Int     => LitValue.I32(v)
    case v: Long    => LitValue.I64(v)
    case v: Double  => LitValue.F64(v)
    case v: Boolean => LitValue.Bool(v)
    case v: String  => LitValue.Str(v)
    case other =>
      throw new IllegalArgumentException(
        s"inline-table cells must be Int/Long/Double/Boolean/String or null; got: $other"
      )

  /** Generic derivation for tuples (multi-column rows). `Tuple` is itself a
    * `Product`, so we read its `productIterator` — each element through
    * [[litOfAny]]. Handles mixed numeric kinds and nulls per the macro's
    * inference (later widened in [[LocalDataBuilder]]). */
  given tupleRows[A <: Tuple]: InlineRows[A] with
    def row(a: A): List[LitValue] = a.productIterator.map(litOfAny).toList

  /** Single-column rows: a bare literal value (FlowExtractor.inlineRow falls
    * back to one cell when the term is not a tuple, lines 538–544). Provided
    * for the cell kinds the macro recognizes. */
  given intRow: InlineRows[Int]         = (a: Int) => List(LitValue.I32(a))
  given longRow: InlineRows[Long]       = (a: Long) => List(LitValue.I64(a))
  given doubleRow: InlineRows[Double]   = (a: Double) => List(LitValue.F64(a))
  given booleanRow: InlineRows[Boolean] = (a: Boolean) => List(LitValue.Bool(a))
  given stringRow: InlineRows[String]   = (a: String) => List(LitValue.Str(a))

/** Holds extracted rows awaiting `.toDF(names*)`, folding the names into the
  * `LocalData` schema exactly as the macro does (FlowExtractor lines 644–654).
  * Without `.toDF`, columns default to `_1.._N` (FlowExtractor line 589;
  * macro case at line 653). */
final case class InlineDf(rows: List[List[LitValue]]):
  def toDF(names: String*): Df =
    LocalDataBuilder.build(rows, Some(names.toList)) match
      case Right(rel) => Df(rel)
      case Left(msg)  => throw new IllegalArgumentException(msg)

  /** Use this `InlineDf` directly as a relation (no `.toDF`): columns default
    * to `_1.._N`, matching `createDataFrame` without `.toDF` (macro line 653).
    * `Df` conversion happens via [[asDf]]. */
  def asDf: Df =
    LocalDataBuilder.build(rows, None) match
      case Right(rel) => Df(rel)
      case Left(msg)  => throw new IllegalArgumentException(msg)

/** Replicates `FlowExtractor.localData`/`inferColType` (FlowExtractor lines
  * 556–597): arity check, names (`toDF` or `_1.._N`), and per-column type
  * inference — numeric widening (I32 < I64 < F64), null takes the column type,
  * mixed non-numeric kinds error. */
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

  def build(rows: List[List[LitValue]], names: Option[List[String]]): Either[String, Rel] =
    if rows.isEmpty then Left("spark.createDataFrame needs at least one row")
    else
      val width = rows.head.size
      if rows.exists(_.size != width) then
        Left("every row in spark.createDataFrame must have the same number of columns")
      else
        val cols = names.getOrElse((1 to width).map(i => s"_$i").toList)
        if cols.size != width then
          Left(s".toDF gave ${cols.size} name(s) for a $width-column inline table")
        else
          val types = (0 until width).toList.foldRight(Right(Nil): Either[String, List[ColType]]) {
            (j, acc) => for { t <- inferColType(rows.map(_(j))); rest <- acc } yield t :: rest
          }
          types.map(ts => Rel.LocalData(cols.zip(ts), rows))
