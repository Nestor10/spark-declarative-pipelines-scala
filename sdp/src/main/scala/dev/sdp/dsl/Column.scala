package dev.sdp.dsl

import dev.sdp.core.algebra.*

/** A column expression: a value wrapping the algebra's expression type `Ex`.
  * Every operator/combinator here builds one `Ex` node by an ordinary method
  * call, so a flow body is evaluated, not inspected.
  *
  * The surface mirrors Spark's own `Column` (D8 — the DSL spells things the way
  * Spark does). Wire names follow Spark's unresolved-function names: `===` →
  * "==", `=!=` → "!=", `&&` → "and", `||` → "or".
  */
final case class Column(ex: Ex):

  // --- binary ops: the wire names Spark's analyzer resolves
  def +(other: Column): Column   = Column(Ex.Fn("+", List(ex, other.ex)))
  def -(other: Column): Column   = Column(Ex.Fn("-", List(ex, other.ex)))
  def *(other: Column): Column   = Column(Ex.Fn("*", List(ex, other.ex)))
  def /(other: Column): Column   = Column(Ex.Fn("/", List(ex, other.ex)))
  def %(other: Column): Column   = Column(Ex.Fn("%", List(ex, other.ex)))
  def >(other: Column): Column   = Column(Ex.Fn(">", List(ex, other.ex)))
  def <(other: Column): Column   = Column(Ex.Fn("<", List(ex, other.ex)))
  def >=(other: Column): Column  = Column(Ex.Fn(">=", List(ex, other.ex)))
  def <=(other: Column): Column  = Column(Ex.Fn("<=", List(ex, other.ex)))
  def ===(other: Column): Column = Column(Ex.Fn("==", List(ex, other.ex)))
  def =!=(other: Column): Column = Column(Ex.Fn("!=", List(ex, other.ex)))
  def &&(other: Column): Column  = Column(Ex.Fn("and", List(ex, other.ex)))
  def ||(other: Column): Column  = Column(Ex.Fn("or", List(ex, other.ex)))

  /** Alias — `c.as("name")` → `Ex.Alias`. */
  def as(name: String): Column = Column(Ex.Alias(ex, name))

  /** `.asc` / `.desc` — produce a [[SortKey]]. */
  def asc: SortKey  = SortKey(ex, descending = false)
  def desc: SortKey = SortKey(ex, descending = true)

  /** ANSI cast to a schema-type token. */
  def cast(to: SchemaType): Column = Column(Ex.Cast(ex, to.colType))

  /** Container access: map key / array index — `getItem` lowers to
    * `ExtractValue(c, key)`. */
  def getItem(key: Column): Column = Column(Ex.ExtractValue(ex, key.ex))

  /** Struct field access — `getField` lowers to
    * `ExtractValue(c, Lit(Str(name)))`. */
  def getField(name: String): Column =
    Column(Ex.ExtractValue(ex, Ex.Lit(LitValue.Str(name))))

  /** Membership in a subquery's result — `.in(sub)` →
    * `Subquery(rel, In(List(lhs)))`. */
  def in(sub: Df): Column =
    Column(Ex.Subquery(sub.rel, SubqueryKind.In(List(ex))))

  /** Membership in a literal set — `isin(values*)` lowers to the SQL `in`
    * function, `Fn("in", lhs :: values)`. */
  def isin(values: Column*): Column =
    Column(Ex.Fn("in", ex :: values.map(_.ex).toList))

  /** Chain a CASE branch — `.when(c, v)` appends `c, v` to the single
    * `Fn("when", [...])`. Only valid onto a `when(...)`. */
  def when(condition: Column, value: Column): Column = ex match
    case Ex.Fn("when", branches, _) =>
      Column(Ex.Fn("when", branches ++ List(condition.ex, value.ex)))
    case _ =>
      throw new IllegalArgumentException(".when(...) chains only onto when(cond, value)")

  /** The CASE else — `.otherwise(e)` appends the else expr to the
    * `Fn("when", [...])`. */
  def otherwise(value: Column): Column = ex match
    case Ex.Fn("when", branches, _) => Column(Ex.Fn("when", branches :+ value.ex))
    case _ =>
      throw new IllegalArgumentException(".otherwise(...) is only valid after when(cond, value)")

  /** Window application — `.over(spec)` →
    * `Window(fn, partitionBy, orderBy, frame = None)`. */
  def over(spec: WindowSpecB): Column =
    Column(Ex.Window(ex, spec.partitionBy, spec.orderBy, None))
