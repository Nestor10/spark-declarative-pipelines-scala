package dev.sdp.dsl.runtime

import scala.NamedTuple.AnyNamedTuple

import dev.sdp.core.algebra.Ex

/** Runtime port of `dev.sdp.dsl.TypedCols` (TypedCols.scala): typed column
  * references over a named-tuple schema.
  *
  * `cols[S]` gives a handle whose fields are exactly `S`'s labels, each typed
  * [[Column]] — so `c.amount` compiles iff `amount` is a column of `S`,
  * autocompletes, and yields `Column(Ex.Col("amount"))`. Wrong names are *type
  * errors* (the `Selectable` field set is computed by the type-level
  * `NamedTuple.Map`). This preserves the macro frontend's compile-time
  * field-name checking — the one place a small `inline` is kept in the runtime
  * world (allowed and expected per the M1 brief).
  *
  * Unlike the macro version (whose `selectDynamic` is a phantom read back out
  * of the AST as a string literal), this one builds the runtime `Column`
  * directly: `selectDynamic("amount")` returns `Column(Ex.Col("amount"))`. So
  * `c.amount` and `col("amount")` are render-identical, exactly as in the macro.
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
