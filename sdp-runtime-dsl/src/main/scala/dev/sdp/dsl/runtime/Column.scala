package dev.sdp.dsl.runtime

import dev.sdp.core.algebra.*

/** Runtime analogue of `dev.sdp.dsl.ExArg`: a value wrapping the algebra's
  * expression type `Ex`. Every operator/combinator here builds the EXACT same
  * `Ex` node the macro's `FlowExtractor.ex` produces for the corresponding
  * surface construct — by ordinary method calls instead of AST reflection.
  *
  * The whole surface mirrors `FlowApi.ExArg` (FlowApi.scala lines 233–276) and
  * the expression cases of `FlowExtractor.ex` (FlowExtractor.scala lines
  * 120–284). Wire names are taken verbatim from `FlowExtractor.BinaryOps`
  * (lines 39–43): `===` → "==", `=!=` → "!=", `&&` → "and", `||` → "or".
  */
final case class Column(ex: Ex):

  // --- binary ops: same wire names as FlowExtractor.BinaryOps (lines 39–43,
  // applied at the `Apply(Select(left, op), List(right))` case, lines 272–276)
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

  /** Alias — `Apply(Select(receiver, "as"), ...)` (FlowExtractor lines 164–168). */
  def as(name: String): Column = Column(Ex.Alias(ex, name))

  /** `.asc` / `.desc` — produce a [[SortKey]] (FlowExtractor.sortKey lines
    * 293–297; the macro reads `Select(inner, "asc"|"desc")`). */
  def asc: SortKey  = SortKey(ex, descending = false)
  def desc: SortKey = SortKey(ex, descending = true)

  /** ANSI cast to a schema-type token (FlowExtractor lines 259–270). */
  def cast(to: SchemaType): Column = Column(Ex.Cast(ex, to.colType))

  /** Container access: map key / array index — `getItem`
    * (FlowExtractor lines 241–245) lowers to `ExtractValue(c, key)`. */
  def getItem(key: Column): Column = Column(Ex.ExtractValue(ex, key.ex))

  /** Struct field access — `getField` (FlowExtractor lines 247–251) lowers to
    * `ExtractValue(c, Lit(Str(name)))`. */
  def getField(name: String): Column =
    Column(Ex.ExtractValue(ex, Ex.Lit(LitValue.Str(name))))

  /** Membership in a subquery's result — `.in(sub)` (FlowExtractor lines
    * 225–229): `Subquery(rel, In(List(lhs)))`. */
  def in(sub: Df): Column =
    Column(Ex.Subquery(sub.rel, SubqueryKind.In(List(ex))))

  /** Membership in a literal set — `Column.isin` (FlowExtractor lines
    * 233–237): lowers to the SQL `in` function `Fn("in", lhs :: values)`. */
  def isin(values: Column*): Column =
    Column(Ex.Fn("in", ex :: values.map(_.ex).toList))

  /** Chain a CASE branch — `.when(c, v)` (FlowExtractor lines 207–215): appends
    * `c, v` to the single `Fn("when", [...])`. Only valid onto a `when(...)`. */
  def when(condition: Column, value: Column): Column = ex match
    case Ex.Fn("when", branches, _) =>
      Column(Ex.Fn("when", branches ++ List(condition.ex, value.ex)))
    case _ =>
      throw new IllegalArgumentException(".when(...) chains only onto when(cond, value)")

  /** The CASE else — `.otherwise(e)` (FlowExtractor lines 216–223): appends the
    * else expr to the `Fn("when", [...])`. */
  def otherwise(value: Column): Column = ex match
    case Ex.Fn("when", branches, _) => Column(Ex.Fn("when", branches :+ value.ex))
    case _ =>
      throw new IllegalArgumentException(".otherwise(...) is only valid after when(cond, value)")

  /** Window application — `.over(spec)` (FlowExtractor lines 253–257):
    * `Window(fn, partitionBy, orderBy, frame=None)`. */
  def over(spec: WindowSpecB): Column =
    Column(Ex.Window(ex, spec.partitionBy, spec.orderBy, None))
