package dev.sdp.connect

import zio.*

/** Reading rows back from a live Spark Connect server — without an Arrow
  * decoder.
  *
  * Spark Connect returns query results as Arrow batches, and this project has
  * no Arrow decoder (nor a reason to grow one — the client pushes graphs, it
  * does not read DataFrames). So the SERVER does the formatting and hands the
  * answer back through the one channel we already decode: its error message.
  * `raise_error` on a collected, sorted string is a full round trip with zero
  * new dependencies.
  *
  * The sentinel is spelled in two pieces so that the literal never appears in
  * the query text itself — error surfaces here echo the offending statement,
  * and a sentinel that matched the echo would let an ANALYSIS failure parse as
  * an empty result set.
  *
  * Shared by [[IcebergAutoCdcE2eSpec]] and [[FullRefreshE2eSpec]]: the trick is
  * subtle enough that a second copy of it would be a second thing to get wrong.
  */
object ServerRead:

  private val Open     = "SDPROWS<"
  private val Close    = ">SDPROWS"
  private val OpenSql  = "concat('SDP','ROWS<')"
  private val CloseSql = "concat('>SDP','ROWS')"

  /** Every row of `table` — a name, or a parenthesised subquery — as
    * `col1,col2,…` strings in sorted order. */
  def rows(
      server: SparkConnectTestServer.Server,
      table: String,
      columns: List[String],
  ): ZIO[Any, Throwable, List[String]] =
    val row = columns.map(c => s"CAST($c AS STRING)").mkString("concat_ws(',', ", ", ", ")")
    val sql =
      s"SELECT raise_error(concat($OpenSql, concat_ws('|', array_sort(collect_list($row))), $CloseSql)) FROM $table"
    CatalogSeeder.run(server.host, server.port, List(sql)).either.flatMap {
      case Right(_) =>
        ZIO.fail(new RuntimeException(s"raise_error did not raise — cannot read $table"))
      case Left(e) =>
        val text = e.describe
        val i    = text.indexOf(Open)
        val j    = if i < 0 then -1 else text.indexOf(Close, i + Open.length)
        if i < 0 || j < 0 then
          ZIO.fail(new RuntimeException(s"could not read $table back; server said: $text"))
        else ZIO.succeed(text.substring(i + Open.length, j).split('|').toList.filter(_.nonEmpty))
    }
