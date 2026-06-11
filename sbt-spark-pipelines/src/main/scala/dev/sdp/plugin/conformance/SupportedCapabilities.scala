package dev.sdp.plugin.conformance

import ConnectTiers.Tier

/** What this project can currently *emit* on the wire, as capability ids
  * (`<message full name>#<field name>`).
  *
  * Rules:
  *   - every claim is verified to exist in [[ConnectInventory]] by the
  *     conformance spec — an upstream rename breaks the claim, loudly;
  *   - claims are added when the emitting code + its tests land, never
  *     speculatively.
  */
object SupportedCapabilities:

  /** Relation oneof entries the encoders can produce. Every entry beyond the
    * first two is **oracle-verified**: `AlgebraOracleSpec` builds it from the
    * Tier-0 ADT and the live server's `AnalyzePlan` resolves it.
    */
  val relations: Set[String] = Set(
    "read",      // Read.named_table + Read.data_source — Rel.NamedTable / Rel.DataSource
    "sql",       // Relation.sql — MV/TV bodies + the algebra's escape hatch
    "project",   // Rel.Project    (oracle-verified, F11)
    "filter",    // Rel.Filter     (oracle-verified, F11)
    "join",      // Rel.Join, all 7 types (oracle-verified, F11/F12)
    "aggregate", // Rel.Aggregate  (oracle-verified, F11)
    "sort",      // Rel.Sort       (oracle-verified, F11)
    "limit",     // Rel.Limit      (oracle-verified, F11)
    "range",                 // Rel.Range              (oracle-verified, F12)
    "offset",                // Rel.Offset             (oracle-verified, F12)
    "tail",                  // Rel.Tail               (oracle-verified, F12)
    "deduplicate",           // Rel.Deduplicate        (oracle-verified, F12)
    "drop",                  // Rel.Drop               (oracle-verified, F12)
    "set_op",                // Rel.SetOp              (oracle-verified, F12)
    "subquery_alias",        // Rel.SubqueryAlias      (oracle-verified, F12)
    "to_df",                 // Rel.ToDF               (oracle-verified, F12)
    "with_columns",          // Rel.WithColumns        (oracle-verified, F12)
    "with_columns_renamed",  // Rel.WithColumnsRenamed (oracle-verified, F12)
    "sample",                // Rel.Sample             (oracle-verified, T1)
    "hint",                  // Rel.Hint               (oracle-verified, T1)
    "repartition",               // Rel.Repartition             (oracle-verified, T1 batch 2)
    "repartition_by_expression", // Rel.RepartitionByExpression (oracle-verified, T1 batch 2)
    "drop_na",                   // Rel.DropNa                  (oracle-verified, T1 batch 2)
    "fill_na",                   // Rel.FillNa                  (oracle-verified, T1 batch 2)
    "describe",                  // Rel.Describe                (oracle-verified, T1 batch 2)
    "summary",                   // Rel.Summary                 (oracle-verified, T1 batch 2)
    "crosstab",                  // Rel.Crosstab                (oracle-verified, T1 batch 2)
    "cov",                       // Rel.Cov                     (oracle-verified, T1 batch 2)
    "corr",                      // Rel.Corr                    (oracle-verified, T1 batch 2)
    "freq_items",                // Rel.FreqItems               (oracle-verified, T1 batch 2)
    "unpivot",                   // Rel.Unpivot                 (oracle-verified, T1 batch 3)
    "transpose",                 // Rel.Transpose               (oracle-verified, T1 batch 3)
    "replace",                   // Rel.Replace                 (oracle-verified, T1 batch 3)
    "sample_by",                 // Rel.SampleBy                (oracle-verified, T1 batch 3)
    "approx_quantile",           // Rel.ApproxQuantile          (oracle-verified, T1 batch 3)
    "collect_metrics",           // Rel.CollectMetrics          (oracle-verified, T1 batch 3)
    "as_of_join",                       // Rel.AsOfJoin       (oracle-verified, T1 final)
    "lateral_join",                     // Rel.LateralJoin    (oracle-verified, T1 final)
    "parse",                            // Rel.Parse          (oracle-verified, T1 final)
    "to_schema",                        // Rel.ToSchema       (oracle-verified, T1 final)
    "local_relation",                   // Rel.LocalRelation — schema-only; Arrow data deferred
    "show_string",                      // Rel.ShowString     (oracle-verified, T1 final)
    "html_string",                      // Rel.HtmlString     (oracle-verified, T1 final)
    "unresolved_table_valued_function", // Rel.Tvf            (oracle-verified, T1 final)
    "catalog",                          // Rel.Catalog (current_database/list_databases/list_tables)
    "with_relations",                   // emitted as the subquery lowering wrapper (plan-id refs)
  )

  /** Expression oneof entries the encoder can produce (oracle-verified
    * through the plans that carry them).
    */
  val expressions: Set[String] = Set(
    "literal",
    "unresolved_attribute",
    "unresolved_function",
    "alias",
    "sort_order",
    "cast",                     // Ex.Cast — ANSI, via type_str DDL
    "expression_string",        // Ex.ExprString — expr("...") escape hatch
    "unresolved_star",          // Ex.Star — count(*)/select(star)
    "unresolved_extract_value", // Ex.ExtractValue — getItem/getField
    "unresolved_regex",         // Ex.ColRegex (programmatic)
    "call_function",            // Ex.CallFn (programmatic)
    "window",                   // Ex.Window — over(partitionBy/orderBy[, frame])
    "lambda_function",                  // Ex.Lam — real Scala lambdas via lam/lam2
    "unresolved_named_lambda_variable", // Ex.LamVar — author param names on the wire
    "subquery_expression",              // Ex.Subquery — exists/scalar/.in (plan-id lowered)
  )

  /** Pipeline command oneof entries the encoder produces. */
  val pipelineCommands: Set[String] = Set(
    "create_dataflow_graph",
    "define_output",
    "define_flow",
    "start_run",
  )

  /** Fully qualified capability claims, for inventory verification. */
  val claims: Set[String] =
    relations.map(f => s"spark.connect.Relation#$f") ++
      expressions.map(f => s"spark.connect.Expression#$f") ++
      pipelineCommands.map(f => s"spark.connect.PipelineCommand#$f")

  /** Per-tier coverage of the Relation surface + visible untriaged counts. */
  def report: String =
    val entries = ConnectTiers.relationOneofEntries
    val byTier  = entries.groupBy(e => ConnectTiers.relationTriage.get(e))

    val tierLines = Tier.values.toList.map { tier =>
      val all       = byTier.getOrElse(Some(tier), Nil)
      val supported = all.filter(relations.contains)
      val pct       = if all.isEmpty then 100 else supported.size * 100 / all.size
      f"  $tier%-14s ${supported.size}%3d / ${all.size}%3d  ($pct%3d%%)  ${all.filterNot(relations.contains).take(6).mkString(", ")}${if all.filterNot(relations.contains).sizeIs > 6 then ", ..." else ""}"
    }

    val untriagedRel  = ConnectTiers.untriagedRelationEntries
    val untriagedExpr = ConnectTiers.untriagedExpressionEntries
    val exprEntries   = ConnectTiers.expressionOneofEntries
    val exprSupported = exprEntries.count(expressions.contains)

    (s"Spark Connect Relation coverage (pinned artifact surface):"
      :: tierLines
      ::: List(
        s"  untriaged relation entries:   ${if untriagedRel.isEmpty then "none" else untriagedRel.mkString(", ")}",
        s"  untriaged expression entries: ${untriagedExpr.size}${if untriagedExpr.isEmpty then "" else untriagedExpr.mkString(" (", ", ", ")")}",
        s"  expressions supported:        $exprSupported / ${exprEntries.size}  (${expressions.toList.sorted.mkString(", ")})",
        s"  pipeline commands supported:  ${pipelineCommands.toList.sorted.mkString(", ")}",
      )).mkString("\n")
