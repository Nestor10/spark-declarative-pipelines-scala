package dev.sdp.core

import dev.sdp.core.algebra.{LitValue, Rel}

/** Build-time guard on inline literal tables (`Rel.LocalData`, the
  * `spark.createDataFrame(...)` surface).
  *
  * Inline data rides the manifest + TASTy as literal rows, and lowers to SQL
  * `VALUES` (D7). That is the right transport for small lookup/seed/enum
  * tables, but genuinely large data inlined this way is an *authoring* smell
  * regardless of transport — it bloats the manifest and the compiled artifact.
  * So we cap it conservatively at build time (accumulating error channel, not
  * short-circuiting). Per D7 the cap ships low on purpose: **raising it later
  * is backward-compatible; lowering it is not.**
  */
object InlineDataGuard:

  /** Max rows in a single inline table. */
  val MaxRows: Int = 1000

  /** Max estimated payload bytes in a single inline table. */
  val MaxBytes: Long = 64L * 1024

  /** Every `InlineTableTooLarge` violation in `relation`, labelled with
    * `flowName`. Empty when every inline table is within the caps. */
  def check(flowName: String, relation: Rel): List[PipelineValidationError] =
    Flow
      .allRels(relation)
      .collect { case ld: Rel.LocalData => ld }
      .flatMap { ld =>
        val rows  = ld.rows.size
        val bytes = estimatedBytes(ld)
        Option.when(rows > MaxRows || bytes > MaxBytes)(
          PipelineValidationError.InlineTableTooLarge(flowName, rows, bytes)
        )
      }

  /** Conservative payload estimate: string length per char, 8 bytes per
    * fixed-width cell, 0 for null. Deterministic (no platform encoding). */
  def estimatedBytes(ld: Rel.LocalData): Long =
    ld.rows.iterator.map(row => row.iterator.map(cellBytes).sum).sum

  private def cellBytes(v: LitValue): Long = v match
    case LitValue.Str(s) => s.length.toLong
    case LitValue.Null   => 0L
    case _               => 8L
