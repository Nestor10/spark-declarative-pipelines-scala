package dev.sdp.core

import dev.sdp.core.algebra.{LitValue, Rel}

import java.nio.charset.StandardCharsets.UTF_8

/** Build-time guard on inline literal tables (`Rel.LocalData`, the
  * `spark.createDataFrame(...)` surface).
  *
  * Inline data rides the fragment string + manifest as literal rows, and lowers to SQL
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
    * `flowName`. Empty when every inline table is within the caps.
    *
    * Walks [[Flow.allRelsDeep]], not `allRels`: an inline table can hide in an
    * expression — `filter(exists(<huge inline table>))` is a relation in
    * expression position, and the structural walk does not enter one. The cap
    * is about how much literal data rides the fragment string and the
    * manifest, and a subquery's rows ride both exactly like a top-level
    * relation's. (P3.1: this was a real bypass, not a hypothetical one.)
    */
  def check(flowName: String, relation: Rel): List[PipelineValidationError] =
    Flow
      .allRelsDeep(relation)
      .collect { case ld: Rel.LocalData => ld }
      .flatMap { ld =>
        val rows  = ld.rows.size
        val bytes = estimatedBytes(ld)
        Option.when(rows > MaxRows || bytes > MaxBytes)(
          PipelineValidationError.InlineTableTooLarge(flowName, rows, bytes)
        )
      }

  /** Conservative payload estimate: a string's real UTF-8 byte count, 8 bytes
    * per fixed-width cell, 0 for null.
    *
    * UTF-8, not `String.length` (P3.1): `length` counts UTF-16 code units, so
    * a table of CJK or emoji cells measured up to 3-4x under its true size and
    * sailed past a cap expressed in BYTES. The charset is named explicitly, so
    * the estimate is deterministic everywhere — it feeds a build-time
    * validation verdict, which must not depend on the platform default.
    */
  def estimatedBytes(ld: Rel.LocalData): Long =
    ld.rows.iterator.map(row => row.iterator.map(cellBytes).sum).sum

  private def cellBytes(v: LitValue): Long = v match
    case LitValue.Str(s) => s.getBytes(UTF_8).length.toLong
    case LitValue.Null   => 0L
    case _               => 8L
