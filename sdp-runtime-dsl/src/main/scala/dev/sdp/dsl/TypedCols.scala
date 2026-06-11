package dev.sdp.dsl

import scala.NamedTuple.AnyNamedTuple

/** F14a spike: typed column references over a named-tuple schema.
  *
  * `cols[S]` gives a phantom handle whose fields are exactly `S`'s labels,
  * each typed `ExArg` — so `c.amount` compiles iff `amount` is a column of
  * `S`, autocompletes in the IDE, and desugars to
  * `selectDynamic("amount")`, which the flow extractor reads as a string
  * literal. Wrong names are *type errors* before extraction even runs.
  *
  * {{{
  * type Orders = (order_id: Long, amount: Long)
  * streamingTableFrom("gold") {
  *   val c = cols[Orders]
  *   stream.table("orders").where(c.amount > lit(0L)).select(c.order_id)
  * }
  * }}}
  *
  * Schema types are ordinary type aliases today; F14b generates them from
  * the live catalog and the pipeline's own inferred schemas
  * (`sdpImportSchemas`).
  */
final class TypedCols[S <: AnyNamedTuple] extends Selectable:
  type Fields = NamedTuple.Map[S, [X] =>> ExArg]
  def selectDynamic(name: String): ExArg = ExArg()

/** Phantom handle for `S`'s columns; see [[TypedCols]]. */
def cols[S <: AnyNamedTuple]: TypedCols[S] = TypedCols[S]()
