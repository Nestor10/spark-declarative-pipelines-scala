package dev.sdp.plugin.conformance

import scala.jdk.CollectionConverters.*

import org.apache.spark.connect.proto as sc

/** Implementation tiers for the Spark Connect wire surface.
  *
  * "100% compliant" is only meaningful **per tier**: the surface includes
  * Python-only payloads and JVM-closure transport that are out of scope by
  * explicit choice, not omission. The triage below makes that choice
  * visible and testable:
  *
  *   - every `Relation` oneof entry MUST be triaged (a test enforces it —
  *     new upstream relation types break the build until consciously
  *     classified);
  *   - `Expression` entries may remain untriaged, but the count is printed
  *     in the coverage report (no silent caps).
  */
object ConnectTiers:

  enum Tier:
    case T0Core        // the relational algebra a first release must speak
    case T1Extended    // long-tail relational + diagnostics
    case T2Streaming   // watermarks & streaming-specific surface
    case T3Deferred    // UDFs / closure transport — deferred by choice
    case NotApplicable // python-only or client-internal plumbing

  import Tier.*

  /** Triage of `spark.connect.Relation`'s `rel_type` oneof, by field name. */
  val relationTriage: Map[String, Tier] = Map(
    // T0 — core relational algebra
    "read"                 -> T0Core,
    "project"              -> T0Core,
    "filter"               -> T0Core,
    "join"                 -> T0Core,
    "set_op"               -> T0Core,
    "sort"                 -> T0Core,
    "limit"                -> T0Core,
    "offset"               -> T0Core,
    "tail"                 -> T0Core,
    "aggregate"            -> T0Core,
    "sql"                  -> T0Core,
    "deduplicate"          -> T0Core,
    "range"                -> T0Core,
    "subquery_alias"       -> T0Core,
    "to_df"                -> T0Core,
    "drop"                 -> T0Core,
    "with_columns"         -> T0Core,
    "with_columns_renamed" -> T0Core,
    // T1 — extended relational + diagnostics
    // local_relation re-triaged T0→T1 (2026-06-06): embedding literal data
    // requires an Arrow IPC writer dependency — not core relational algebra.
    "local_relation"                    -> T1Extended,
    "sample"                            -> T1Extended,
    "unpivot"                           -> T1Extended,
    "transpose"                         -> T1Extended,
    "to_schema"                         -> T1Extended,
    "repartition"                       -> T1Extended,
    "repartition_by_expression"         -> T1Extended,
    "hint"                              -> T1Extended,
    "as_of_join"                        -> T1Extended,
    "lateral_join"                      -> T1Extended,
    "with_relations"                    -> T1Extended,
    "unresolved_table_valued_function"  -> T1Extended,
    "fill_na"                           -> T1Extended,
    // with_relations: not an authoring surface, but WE EMIT IT — the
    // encoder lowers Ex.Subquery to SubqueryExpression(plan_id) + a
    // WithRelations wrapper carrying the referenced plans (re-triaged
    // NA→T1 2026-06-07 when subqueries shipped).
    "drop_na"                           -> T1Extended,
    "replace"                           -> T1Extended,
    "summary"                           -> T1Extended,
    "crosstab"                          -> T1Extended,
    "describe"                          -> T1Extended,
    "cov"                               -> T1Extended,
    "corr"                              -> T1Extended,
    "approx_quantile"                   -> T1Extended,
    "freq_items"                        -> T1Extended,
    "sample_by"                         -> T1Extended,
    "show_string"                       -> T1Extended,
    "html_string"                       -> T1Extended,
    "collect_metrics"                   -> T1Extended,
    "parse"                             -> T1Extended,
    "catalog"                           -> T1Extended,
    "nearest_by_join"                   -> T1Extended,
    "zip"                               -> T1Extended,
    // T2 — streaming
    "with_watermark" -> T2Streaming,
    // T3 — UDF / closure transport, deferred by explicit choice
    "map_partitions"                            -> T3Deferred,
    "group_map"                                 -> T3Deferred,
    "co_group_map"                              -> T3Deferred,
    "common_inline_user_defined_table_function" -> T3Deferred,
    "common_inline_user_defined_data_source"    -> T3Deferred,
    "ml_relation"                               -> T3Deferred,
    // N/A — python-only or protocol/client internals
    "apply_in_pandas_with_state"   -> NotApplicable,
    "cached_local_relation"        -> NotApplicable,
    "cached_remote_relation"       -> NotApplicable,
    "chunked_cached_local_relation"-> NotApplicable,
    "relation_changes"             -> NotApplicable,
    "extension"                    -> NotApplicable,
    "unknown"                      -> NotApplicable,
  )

  /** Seed triage of `spark.connect.Expression`'s oneof. Untriaged entries
    * are allowed here — but counted, visibly, in the report.
    */
  val expressionTriage: Map[String, Tier] = Map(
    "literal"               -> T0Core,
    "unresolved_attribute"  -> T0Core,
    "unresolved_function"   -> T0Core,
    "unresolved_star"       -> T0Core,
    "alias"                 -> T0Core,
    "cast"                  -> T0Core,
    "sort_order"            -> T0Core,
    "expression_string"     -> T0Core,
    "unresolved_regex"      -> T1Extended,
    "unresolved_extract_value" -> T1Extended,
    "update_fields"         -> T1Extended,
    "lambda_function"       -> T1Extended,
    "unresolved_named_lambda_variable" -> T1Extended,
    "window"                -> T1Extended,
    "call_function"         -> T1Extended,
    "named_argument_expression" -> T1Extended,
    "common_inline_user_defined_function" -> T3Deferred,
    "extension"             -> NotApplicable,
    // surfaced by the harness from the 4.1.2 artifact (2026-06-06), triaged:
    "subquery_expression"            -> T1Extended,   // scalar/EXISTS subqueries
    "typed_aggregate_expression"     -> T3Deferred,   // typed encoders = closure territory
    "merge_action"                   -> NotApplicable, // MERGE INTO internals, not pipeline flows
    "direct_shuffle_partition_id"    -> NotApplicable, // engine-internal partitioning
  )

  /** The `rel_type` oneof entry names, straight from the live descriptor. */
  def relationOneofEntries: List[String] =
    oneofEntries(sc.Relation.getDescriptor, "rel_type")

  /** The expression oneof entry names (`expr_type`). */
  def expressionOneofEntries: List[String] =
    oneofEntries(sc.Expression.getDescriptor, "expr_type")

  def untriagedRelationEntries: List[String] =
    relationOneofEntries.filterNot(relationTriage.contains)

  def untriagedExpressionEntries: List[String] =
    expressionOneofEntries.filterNot(expressionTriage.contains)

  private def oneofEntries(
      message: com.google.protobuf.Descriptors.Descriptor,
      oneofName: String,
  ): List[String] =
    message.getOneofs.asScala
      .find(_.getName == oneofName)
      .map(_.getFields.asScala.map(_.getName).toList.sorted)
      .getOrElse(Nil)
