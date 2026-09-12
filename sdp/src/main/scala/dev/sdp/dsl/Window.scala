package dev.sdp.dsl

import dev.sdp.core.algebra.*

/** Window specifications — the `org.apache.spark.sql.expressions.Window`
  * surface (D8), spelled `window` or `Window`.
  *
  * `window.partitionBy(cols*)`, `window.orderBy(keys*)`, and `.orderBy(keys*)`
  * chained onto a partition spec all build a [[WindowSpecB]]; order keys come
  * from `Column#asc`/`Column#desc` ([[SortKey]]) or a bare `Column`
  * (ascending, Spark's default).
  */
final case class WindowSpecB(partitionBy: List[Ex], orderBy: List[SortKey]):
  /** `.orderBy(col.asc, other.desc)` — explicit sort keys. */
  def orderBy(keys: SortKey*): WindowSpecB = copy(orderBy = orderBy ++ keys)

  /** `.orderBy(col1, cols*)` — bare columns order ascending (Spark default). */
  def orderBy(col1: Column, cols: Column*): WindowSpecB =
    copy(orderBy = orderBy ++ (col1 +: cols).map(c => SortKey(c.ex)))

/** `org.apache.spark.sql.expressions.Window` mirror. */
object Window:
  def partitionBy(columns: Column*): WindowSpecB =
    WindowSpecB(columns.map(_.ex).toList, Nil)
  def orderBy(keys: SortKey*): WindowSpecB = WindowSpecB(Nil, keys.toList)
  def orderBy(col1: Column, cols: Column*): WindowSpecB =
    WindowSpecB(Nil, (col1 +: cols).map(c => SortKey(c.ex)).toList)

/** Lowercase spelling of [[Window]] — both are accepted. */
val window: Window.type = Window
