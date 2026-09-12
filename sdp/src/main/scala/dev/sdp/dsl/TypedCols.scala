package dev.sdp.dsl

import scala.NamedTuple.AnyNamedTuple

import dev.sdp.core.algebra.Ex

/** Typed column references over a named-tuple schema.
  *
  * `cols[S]` gives a handle whose fields are exactly `S`'s labels, each typed
  * [[Column]] — so `c.amount` compiles iff `amount` is a column of `S`,
  * autocompletes, and yields `Column(Ex.Col("amount"))`. A wrong name is a
  * *type error* at `compile`, before any validation runs.
  *
  * No macro is involved: this is `Selectable` plus Scala 3 named tuples. The
  * field set is computed by the type-level `NamedTuple.Map`, and
  * `selectDynamic("amount")` simply returns `Column(Ex.Col("amount"))` — so
  * `c.amount` and `col("amount")` are render-identical.
  *
  * {{{
  * type Orders = (order_id: Long, amount: Long)
  * streamingTable("gold") {
  *   val c = cols[Orders]
  *   stream.table("orders").where(c.amount > lit(0L)).select(c.order_id)
  * }
  * }}}
  */
final class TypedCols[S <: AnyNamedTuple] extends Selectable:
  type Fields = NamedTuple.Map[S, [X] =>> Column]
  def selectDynamic(name: String): Column = Column(Ex.Col(name))

/** Phantom handle for `S`'s columns; see [[TypedCols]]. */
def cols[S <: AnyNamedTuple]: TypedCols[S] = TypedCols[S]()
