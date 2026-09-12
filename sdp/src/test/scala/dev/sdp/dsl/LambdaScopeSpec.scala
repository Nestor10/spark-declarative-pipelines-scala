package dev.sdp.dsl

import dev.sdp.core.algebra.{Ex, RelCodec}
import dev.sdp.dsl.functions.*
import zio.test.*

/** Higher-order-function lambda naming (review item 3).
  *
  * The bug: `transform`/`filter`/`forall` synthesized the fixed name "x", so a
  * NESTED HOF bound `x` twice and the inner `Ex.Lam` shadowed the outer
  * variable — the author's reference to the outer element silently resolved to
  * the inner one server-side. These tests pin both halves of the fix: nested
  * lambdas get distinct `LamVar`s, and an unnested lambda keeps the
  * conventional spelling (the frozen golden oracle depends on it).
  */
object LambdaScopeSpec extends ZIOSpecDefault:

  /** Every lambda parameter name in the tree, outermost first. */
  private def lamParams(ex: Ex): List[List[String]] = ex match
    case Ex.Lam(params, body) => params :: lamParams(body)
    case Ex.Fn(_, args, _)    => args.flatMap(lamParams)
    case Ex.Alias(inner, _)   => lamParams(inner)
    case _                    => Nil

  /** Every lambda-variable reference in the tree. */
  private def lamVars(ex: Ex): List[String] = ex match
    case Ex.LamVar(name)    => List(name)
    case Ex.Lam(_, body)    => lamVars(body)
    case Ex.Fn(_, args, _)  => args.flatMap(lamVars)
    case Ex.Alias(inner, _) => lamVars(inner)
    case _                  => Nil

  def spec = suite("HOF lambda variables are collision-free")(
    test("a nested transform binds two DISTINCT lambda variables") {
      val ex = transform(col("xs"), x => transform(x, y => y + x)).ex
      val params = lamParams(ex)
      assertTrue(
        params == List(List("x"), List("x_1")),
        // the inner body references BOTH bindings — the outer one by its own
        // name, which is exactly what the shadowed version lost
        lamVars(ex).toSet == Set("x", "x_1"),
      )
    },
    test("a nested filter inside aggregate avoids both enclosing names") {
      val ex = aggregate(col("xs"), lit(0L), (acc, x) => acc + size(filter(x, e => e > lit(0)))).ex
      assertTrue(lamParams(ex) == List(List("acc", "x"), List("x_1")))
    },
    test("three levels of nesting all differ") {
      val ex = transform(col("xs"), a => transform(a, b => transform(b, c => c + a))).ex
      assertTrue(
        lamParams(ex) == List(List("x"), List("x_1"), List("x_2")),
        // each level is read by the level below it, and the innermost body
        // still reaches the OUTERMOST binding by its own name
        lamVars(ex) == List("x", "x_1", "x_2", "x"),
      )
    },
    test("an unnested lambda keeps its conventional name (golden renders unchanged)") {
      assertTrue(
        lamParams(transform(col("xs"), x => x * lit(2)).ex) == List(List("x")),
        lamParams(filter(col("xs"), x => x > lit(0)).ex) == List(List("x")),
        lamParams(forall(col("xs"), x => x > lit(0)).ex) == List(List("x")),
        lamParams(zip_with(col("a"), col("b"), (l, r) => l + r).ex) == List(List("l", "r")),
        lamParams(aggregate(col("xs"), lit(0L), (acc, x) => acc + x).ex) == List(List("acc", "x")),
        lamParams(lam(x => x + lit(1)).ex) == List(List("x")),
        lamParams(lam("item")(i => i + lit(1)).ex) == List(List("item")),
        lamParams(lam2((l, r) => l + r).ex) == List(List("l", "r")),
      )
    },
    test("sibling (non-nested) lambdas both keep the plain name") {
      val ex = fn("concat", transform(col("a"), x => x), transform(col("b"), x => x)).ex
      assertTrue(lamParams(ex) == List(List("x"), List("x")))
    },
    test("the freshened name survives the codec round-trip") {
      val ex       = transform(col("xs"), x => transform(x, y => y + x)).ex
      val relation = spark.table("t").select(Column(ex).as("nested")).rel
      assertTrue(
        RelCodec.parse(RelCodec.render(relation)) == Right(relation),
        RelCodec.render(relation).contains("x_1"),
      )
    },
    test("scope is restored, so a later lambda is not freshened") {
      val nested = transform(col("xs"), x => transform(x, y => y)).ex
      val after  = transform(col("ys"), x => x).ex
      assertTrue(
        lamParams(nested) == List(List("x"), List("x_1")),
        lamParams(after) == List(List("x")),
      )
    },
  )
