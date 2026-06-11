package dev.sdp.core

import dev.sdp.core.algebra.{Ex, Rel}

/** What a [[Flow]] *does* with its target.
  *
  * Manifest history: format v2 knew only one shape — a flow that writes a
  * computed [[algebra.Rel]] into its target. Spark 4.2 adds AUTO CDC (the
  * donated DLT `apply_changes`): a declarative MERGE/SCD flow that streams a
  * CDC `source` into a streaming-table target, keyed by row identity and
  * ordered by a sequence expression. AUTO CDC has no defining `Rel` — its
  * "body" is the set of CDC parameters below.
  *
  * The split keeps the common path byte-identical: a [[WriteRelation]] flow
  * with `once = false` still renders as the v2 `flow|name|target|<rel>` line,
  * so every manifest written before this change parses unchanged.
  */
enum FlowDetails:

  /** A flow whose body is a computed relation written into the target — the
    * only shape format v2 could express. */
  case WriteRelation(relation: Rel)

  /** AUTO CDC (Spark 4.2, `pipelines.proto` `AutoCdcFlowDetails`). Streams the
    * `source` dataset into the target, MERGEing rows by `keys` ordered by
    * `sequenceBy`. Field numbers below cite the frozen v4.2.0-rc1 proto so the
    * encoder gate can map 1:1 when the wire dep bumps.
    *
    * @param source     CDC source dataset to stream from (proto field 1)
    * @param keys       row identity in source & target (field 2)
    * @param sequenceBy ordering of source events (field 3)
    * @param applyAsDeletes   delete condition (field 6)
    * @param applyAsTruncates truncate condition (field 7)
    * @param columnList       include columns (field 8)
    * @param exceptColumnList exclude columns (field 9)
    * @param ignoreNullUpdatesColumnList       (field 14)
    * @param ignoreNullUpdatesExceptColumnList (field 15)
    * @param scdType    SCD strategy (field 10); only SCD_TYPE_1 exists so far
    */
  case AutoCdc(
      source: String,
      keys: List[Ex],
      sequenceBy: Ex,
      applyAsDeletes: Option[Ex] = None,
      applyAsTruncates: Option[Ex] = None,
      columnList: List[Ex] = Nil,
      exceptColumnList: List[Ex] = Nil,
      ignoreNullUpdatesColumnList: List[Ex] = Nil,
      ignoreNullUpdatesExceptColumnList: List[Ex] = Nil,
      scdType: ScdType = ScdType.Scd1,
  )

/** Slowly-changing-dimension strategy. The proto's `SCDType` enum has only
  * `SCD_TYPE_1 = 1` so far (`SCD_TYPE_UNSPECIFIED = 0` is the absent default);
  * we model the one real value. */
enum ScdType:
  case Scd1
