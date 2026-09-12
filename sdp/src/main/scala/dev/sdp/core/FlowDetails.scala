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
    * @param scdType    SCD strategy (field 10)
    * @param trackHistoryColumnList       SCD2 only: columns whose value change
    *                                     opens a new history record (field 11);
    *                                     empty = track every selected column
    * @param trackHistoryExceptColumnList SCD2 only: columns excluded from
    *                                     history tracking (field 12); mutually
    *                                     exclusive with the list above
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
      trackHistoryColumnList: List[Ex] = Nil,
      trackHistoryExceptColumnList: List[Ex] = Nil,
  )

/** Slowly-changing-dimension strategy — the proto's `SCDType` enum
  * (`SCD_TYPE_UNSPECIFIED = 0` is the absent default, which the server reads as
  * SCD1, so we always say which one we mean).
  *
  * `Scd2` is authored and validated offline today but **cannot go on the wire
  * yet**: `SCD_TYPE_2 = 2` and the two `track_history_*` lists are master-only
  * (SPARK-58247, absent from the published 4.2.0 proto). The encoder detects
  * their presence by descriptor and refuses until the pin carries them — see
  * `dev.sdp.connect.Scd2Wire`. */
enum ScdType:
  case Scd1
  case Scd2
