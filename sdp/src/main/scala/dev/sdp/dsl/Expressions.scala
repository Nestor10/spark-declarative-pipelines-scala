package dev.sdp.dsl

import dev.sdp.core.algebra.*
import dev.sdp.core.algebra.RelCodec

/** Top-level expression constructors: `col`, `lit`, `star`, `expr`, `fn`,
  * `lam`/`lam2`, and the subquery combinators `exists`/`scalar`. Each builds one
  * `Ex` node directly.
  */

/** Column reference — `col(name)` → `Ex.Col(name)`. */
def col(name: String): Column = Column(Ex.Col(name))

/** `*` — `star` → `Ex.Star(None)`. */
val star: Column = Column(Ex.Star(None))

// lit overloads — one per supported literal kind. Each takes an ordinary Scala
// value, so a computed value is as welcome as a constant.
def lit(value: Int): Column     = Column(Ex.Lit(LitValue.I32(value)))
def lit(value: Long): Column    = Column(Ex.Lit(LitValue.I64(value)))
def lit(value: Double): Column  =
  Column(Ex.Lit(LitValue.F64(RelCodec.requireFinite("lit(Double)", value))))
def lit(value: Boolean): Column = Column(Ex.Lit(LitValue.Bool(value)))
def lit(value: String): Column  = Column(Ex.Lit(LitValue.Str(value)))

/** SQL-fragment escape hatch — `expr(sql)` → `Ex.ExprString(sql)`. */
def expr(sql: String): Column = Column(Ex.ExprString(sql))

/** Any Spark SQL function by name — `fn(name, args*)` → `Ex.Fn(name, args)`. */
def fn(name: String, args: Column*): Column = Column(Ex.Fn(name, args.map(_.ex).toList))

/** A single-parameter lambda for higher-order functions. The Scala parameter
  * name is erased at runtime, so the wire `LamVar` gets the conventional name
  * "x" — freshened automatically when lambdas nest, see [[LambdaScope]]. */
def lam(f: Column => Column): Column =
  Column(LambdaScope.lam(List("x"))(ps => f(ps.head)))

/** A named single-parameter lambda — name the wire `LamVar` explicitly when
  * the generated "x" is not the spelling you want. */
def lam(param: String)(f: Column => Column): Column =
  Column(LambdaScope.lam(List(param))(ps => f(ps.head)))

/** A two-parameter lambda — `lam2`. */
def lam2(f: (Column, Column) => Column): Column =
  Column(LambdaScope.lam(List("l", "r"))(ps => f(ps.head, ps(1))))

/** Named two-parameter lambda — runtime escape for matching source names. */
def lam2(p1: String, p2: String)(f: (Column, Column) => Column): Column =
  Column(LambdaScope.lam(List(p1, p2))(ps => f(ps.head, ps(1))))

/** EXISTS subquery — `exists(sub)` → `Subquery(rel, Exists)`. */
def exists(sub: Df): Column = Column(Ex.Subquery(sub.rel, SubqueryKind.Exists))

/** Scalar subquery — `scalar(sub)` → `Subquery(rel, Scalar)`. */
def scalar(sub: Df): Column = Column(Ex.Subquery(sub.rel, SubqueryKind.Scalar))

// ====================================================================
// schema-type tokens (cast targets / withSchema field types)
// ====================================================================

/** The cast / `withSchema` type tokens; each carries its `ColType`. */
final case class SchemaType(colType: ColType)

val bool: SchemaType      = SchemaType(ColType.Bool)
val int: SchemaType       = SchemaType(ColType.I32)
val long: SchemaType      = SchemaType(ColType.I64)
val double: SchemaType    = SchemaType(ColType.F64)
val string: SchemaType    = SchemaType(ColType.Str)
val timestamp: SchemaType = SchemaType(ColType.Timestamp)
val date: SchemaType      = SchemaType(ColType.Date)

/** A declared source column for `withSchema` — `field(name, type)`. */
final case class SchemaField(name: String, colType: ColType)

def field(name: String, tpe: SchemaType): SchemaField = SchemaField(name, tpe.colType)
