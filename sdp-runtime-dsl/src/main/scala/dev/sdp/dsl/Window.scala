package dev.sdp.dsl

import dev.sdp.core.algebra.*

/** Runtime mirror of `dev.sdp.dsl.WindowSpec` + `Window`/`window`
  * (FlowApi.scala lines 278–290), interpreted at compile time by
  * `FlowExtractor.windowSpec` (FlowExtractor.scala lines 461–478).
  *
  * The macro's `windowSpec` recognizes `window.partitionBy(cols*)`,
  * `window.orderBy(keys*)`, and `.orderBy(keys*)` chained onto a partition
  * spec; order keys come from `Column#asc`/`Column#desc` ([[SortKey]]) or a
  * bare `Column` (ascending, Spark default — sortKey lines 293–297).
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

/** Pre-facade lowercase spelling; the macro accepts `window` and `Window`
  * (FlowExtractor lines 464–466). A `val` alias, matching FlowApi line 290. */
val window: Window.type = Window
