package dev.sdp.dsl

import dev.sdp.core.algebra.*

/** Top-level expression constructors mirroring the ones in `dev.sdp.dsl`
  * (`FlowApi.scala`): `col`, `lit`, `star`, `expr`, `fn`, `lam`/`lam2`, and the
  * subquery combinators `exists`/`scalar`. Each builds the EXACT `Ex` node the
  * macro's `FlowExtractor.ex` produces (FlowExtractor.scala lines 120–284).
  */

/** Column reference — `Apply(Ident("col"), ...)` (FlowExtractor lines
  * 144–145): `Ex.Col(name)`. */
def col(name: String): Column = Column(Ex.Col(name))

/** `*` — `Ident("star")` (FlowExtractor line 239): `Ex.Star(None)`. */
val star: Column = Column(Ex.Star(None))

// lit overloads — same literal kinds the macro's `lit` recognizes
// (FlowExtractor lines 147–156). Each takes a constant Scala value (the
// runtime loses the macro's compile-time "must be literal" guard — documented).
def lit(value: Int): Column     = Column(Ex.Lit(LitValue.I32(value)))
def lit(value: Long): Column    = Column(Ex.Lit(LitValue.I64(value)))
def lit(value: Double): Column  = Column(Ex.Lit(LitValue.F64(value)))
def lit(value: Boolean): Column = Column(Ex.Lit(LitValue.Bool(value)))
def lit(value: String): Column  = Column(Ex.Lit(LitValue.Str(value)))

/** SQL-fragment escape hatch — `Apply(Ident("expr"), ...)` (FlowExtractor lines
  * 170–171): `Ex.ExprString(sql)`. */
def expr(sql: String): Column = Column(Ex.ExprString(sql))

/** Any Spark SQL function by name — `Apply(Ident("fn"), name :: args)`
  * (FlowExtractor lines 158–162): `Ex.Fn(name, args)`. */
def fn(name: String, args: Column*): Column = Column(Ex.Fn(name, args.map(_.ex).toList))

/** A single-parameter lambda for higher-order functions — `lam`
  * (FlowExtractor lines 175–187): the author's parameter name becomes the wire
  * `LamVar`. The synthetic name "x" matches the conventional spelling; the
  * macro uses the real source name, so HOF lambdas in fixtures must use this
  * same name to stay render-identical (see report). */
def lam(f: Column => Column): Column =
  Column(Ex.Lam(List("x"), f(Column(Ex.LamVar("x"))).ex))

/** A named single-parameter lambda — runtime escape so the wire `LamVar` can
  * match the macro's source-derived parameter name exactly. Not on the macro
  * surface; runtime-only convenience for render-faithful HOF fixtures. */
def lam(param: String)(f: Column => Column): Column =
  Column(Ex.Lam(List(param), f(Column(Ex.LamVar(param))).ex))

/** A two-parameter lambda — `lam2` (FlowExtractor lines 175–187). */
def lam2(f: (Column, Column) => Column): Column =
  Column(Ex.Lam(List("l", "r"), f(Column(Ex.LamVar("l")), Column(Ex.LamVar("r"))).ex))

/** Named two-parameter lambda — runtime escape for matching source names. */
def lam2(p1: String, p2: String)(f: (Column, Column) => Column): Column =
  Column(Ex.Lam(List(p1, p2), f(Column(Ex.LamVar(p1)), Column(Ex.LamVar(p2))).ex))

/** EXISTS subquery — `Apply(Ident("exists"), List(rel))` (FlowExtractor lines
  * 192–195): `Subquery(rel, Exists)`. */
def exists(sub: Df): Column = Column(Ex.Subquery(sub.rel, SubqueryKind.Exists))

/** Scalar subquery — `Apply(Ident("scalar"), List(rel))` (FlowExtractor lines
  * 192–195): `Subquery(rel, Scalar)`. */
def scalar(sub: Df): Column = Column(Ex.Subquery(sub.rel, SubqueryKind.Scalar))

// ====================================================================
// schema-type tokens (cast targets / withSchema field types)
// ====================================================================

/** Runtime mirror of `FlowApi.SchemaType` — the cast/schema type tokens. The
  * macro reads these as `Ident(tok)` against `FlowExtractor.SchemaTypes`
  * (lines 55–63); here each token carries its `ColType` directly. */
final case class SchemaType(colType: ColType)

val bool: SchemaType      = SchemaType(ColType.Bool)
val int: SchemaType       = SchemaType(ColType.I32)
val long: SchemaType      = SchemaType(ColType.I64)
val double: SchemaType    = SchemaType(ColType.F64)
val string: SchemaType    = SchemaType(ColType.Str)
val timestamp: SchemaType = SchemaType(ColType.Timestamp)
val date: SchemaType      = SchemaType(ColType.Date)

/** A declared source column for `withSchema` — `field(name, type)`
  * (FlowExtractor lines 724–735). */
final case class SchemaField(name: String, colType: ColType)

def field(name: String, tpe: SchemaType): SchemaField = SchemaField(name, tpe.colType)
