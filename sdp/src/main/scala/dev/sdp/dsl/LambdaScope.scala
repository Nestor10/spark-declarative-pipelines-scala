package dev.sdp.dsl

import dev.sdp.core.algebra.*

/** Collision-free naming for higher-order-function lambda variables.
  *
  * The problem: the HOF surface is Spark-faithful (D8), so the author writes a
  * real Scala lambda — `transform(xs, x => ...)` — and the *Scala* parameter
  * name is erased by the time the builder runs. The builder therefore
  * synthesizes the conventional wire name ("x", "acc"/"x", "l"/"r"). Two nested
  * HOFs then synthesize the SAME name, and the inner `Ex.Lam` shadows the outer
  * binding: `transform(xs, x => filter(x, y => y > x))` rendered both variables
  * as `x`, so the server resolved the outer reference to the INNER binding — a
  * clean compile and a wrong plan.
  *
  * The fix is a dynamically-scoped set of the lambda names currently in scope.
  * Constructing a lambda freshens each parameter against that set — an unused
  * name is kept verbatim, a taken one gets the lowest free `_<n>` suffix (the
  * same shape Spark's own `UnresolvedNamedLambdaVariable.freshVarName` uses) —
  * and the body is evaluated with the new names added. A lambda that is NOT
  * nested sees an empty set, so it keeps its conventional name and every
  * existing render stays byte-identical (the frozen golden oracle must not
  * churn); only a nested lambda is renamed, which is exactly the broken case.
  *
  * Why a `ThreadLocal`: the author's lambda has type `Column => Column` — fixed
  * by the Spark-faithful surface — so there is no parameter to thread a scope
  * through. Plan construction is synchronous and single-threaded per flow body,
  * and the scope is restored in a `finally`, so this is dynamic scoping (the
  * sync analogue of a `FiberRef`) rather than shared mutable state: the same
  * body always builds the same tree.
  */
private[dsl] object LambdaScope:

  private val inScope: ThreadLocal[Set[String]] =
    ThreadLocal.withInitial(() => Set.empty[String])

  /** Build `Ex.Lam` over `params`, freshened against the enclosing lambdas, and
    * evaluate `body` with those names in scope. `body` receives one `Column`
    * per parameter, in order. */
  def lam(params: List[String])(body: List[Column] => Column): Ex =
    val outer = inScope.get
    // Fold so that a lambda's own parameters are also distinct from each other.
    val names = params.foldLeft(List.empty[String])((acc, p) => acc :+ fresh(p, outer ++ acc))
    inScope.set(outer ++ names)
    try Ex.Lam(names, body(names.map(n => Column(Ex.LamVar(n)))).ex)
    finally inScope.set(outer)

  /** `base` when free, else `base_1`, `base_2`, … — the first one not taken. */
  private def fresh(base: String, taken: Set[String]): String =
    if !taken.contains(base) then base
    else Iterator.from(1).map(i => s"${base}_$i").find(n => !taken.contains(n)).get
