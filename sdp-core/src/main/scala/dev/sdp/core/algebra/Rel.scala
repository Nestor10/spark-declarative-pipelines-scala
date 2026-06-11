package dev.sdp.core.algebra

/** The Tier-0 relational algebra: a pure Scala 3 tree that mirrors the
  * `spark.connect.Relation` wire surface, one case per supported capability.
  *
  * This is the heart of the project. Authors (via DSL combinators, F11b)
  * build these trees; macros embed them in TASTy; the plugin encodes them to
  * protobuf; Spark's server analyzes them. Because the tree is a pure value
  * with zero dependencies:
  *
  *   - it serializes canonically (manifest v2) and travels the same TASTy
  *     channel as everything else — no runtime evaluation, anywhere;
  *   - equality is structural, so plans are testable like data;
  *   - the conformance matrix maps 1:1 — each case below is a claimed
  *     capability, each claim oracle-verified via `AnalyzePlan`.
  *
  * Tier-0 scope (per `ConnectTiers`): reads, projection, filtering, joins,
  * aggregation, sort, limit, and the SQL escape hatch. Everything else
  * arrives tier by tier.
  */
enum Rel:
  /** Read a dataset by name; `streaming` controls `Read.is_streaming`. */
  case NamedTable(name: String, streaming: Boolean)

  /** Read an external data source by format (`rate`, `kafka`, `parquet`,
    * ...). This is the streaming *leaf* — the only relation that introduces
    * data into a pipeline from outside the graph.
    *
    * `schema` is *build-time* metadata for gradual column checking (sources
    * are where shapes are unknowable without declaration); it is not emitted
    * on the wire — streaming sources like `rate` reject user schemas.
    */
  case DataSource(
      format: String,
      options: Map[String, String],
      streaming: Boolean,
      // Local-inference shape (gradual schema checks). Lossy for types ColType
      // doesn't model (DECIMAL, ARRAY, ...) — those land as Unknown.
      schema: List[(String, ColType)] = Nil,
      // The raw DDL the author wrote, preserved verbatim and emitted on the
      // wire (`Read.DataSource.schema`) so file/CSV/JSON streaming sources
      // resolve server-side. Verbatim, so DECIMAL/ARRAY/STRUCT survive intact —
      // unlike a round-trip through the lossy ColType list above.
      schemaDdl: Option[String] = None,
  )

  /** The escape hatch: any SQL the server can parse. */
  case Sql(query: String)

  /** Generate rows `start until end by step` — the batch test/dimension leaf. */
  case Range(start: Long, end: Long, step: Long)

  case Project(input: Rel, columns: List[Ex])
  case Filter(input: Rel, condition: Ex)
  case Join(left: Rel, right: Rel, condition: Option[Ex], joinType: JoinType)
  case Aggregate(input: Rel, groupBy: List[Ex], aggregates: List[Ex])
  case Sort(input: Rel, order: List[SortKey])
  case Limit(input: Rel, n: Int)
  case Offset(input: Rel, n: Int)
  case Tail(input: Rel, n: Int)

  /** Empty `columns` deduplicates on all columns (`distinct`). */
  case Deduplicate(input: Rel, columns: List[String])

  case Drop(input: Rel, columnNames: List[String])
  case SetOp(left: Rel, right: Rel, op: SetOpType, all: Boolean)
  case SubqueryAlias(input: Rel, alias: String)
  case ToDF(input: Rel, columnNames: List[String])

  /** Add or replace columns; pairs are (name, expression), order preserved. */
  case WithColumns(input: Rel, columns: List[(String, Ex)])

  /** Rename columns; pairs are (existing, new), order preserved. */
  case WithColumnsRenamed(input: Rel, renames: List[(String, String)])

  /** Random sample of `fraction` (0..1) of the input, optionally seeded. */
  case Sample(input: Rel, fraction: Double, seed: Option[Long])

  /** Query hint, e.g. `broadcast`. */
  case Hint(input: Rel, name: String, parameters: List[Ex])

  /** Coalesce/shuffle into `n` partitions. */
  case Repartition(input: Rel, numPartitions: Int, shuffle: Boolean)

  /** Repartition by expressions (hash partitioning). */
  case RepartitionByExpression(input: Rel, partitionExprs: List[Ex], numPartitions: Option[Int])

  /** Drop rows with nulls in `cols` (all columns when empty). */
  case DropNa(input: Rel, cols: List[String])

  /** Fill nulls in `cols` (all columns when empty) with `value`. */
  case FillNa(input: Rel, cols: List[String], value: LitValue)

  /** Melt: keep `ids`, fold `values` columns into (variable, value) rows.
    * Empty `values` lets the server unpivot every non-id column.
    */
  case Unpivot(
      input: Rel,
      ids: List[Ex],
      values: List[Ex],
      variableColumnName: String,
      valueColumnName: String,
  )

  /** Transpose rows/columns around `indexColumns`. */
  case Transpose(input: Rel, indexColumns: List[Ex])

  /** Replace values in `cols` (all when empty): (old, new) literal pairs. */
  case Replace(input: Rel, cols: List[String], replacements: List[(LitValue, LitValue)])

  /** Stratified sample: per-stratum fractions on `col`. */
  case SampleBy(input: Rel, col: Ex, fractions: List[(LitValue, Double)], seed: Option[Long])

  /** Approximate quantiles of `cols` at `probabilities`. */
  case ApproxQuantile(input: Rel, cols: List[String], probabilities: List[Double], relativeError: Double)

  /** Named observation point: metrics computed over the flow, surfaced in
    * query progress — the pipeline observability hook.
    */
  case CollectMetrics(input: Rel, name: String, metrics: List[Ex])

  /** Time-series as-of join: each left row matches the nearest right row by
    * the as-of keys. `direction`: backward | forward | nearest.
    */
  case AsOfJoin(
      left: Rel,
      right: Rel,
      leftAsOf: Ex,
      rightAsOf: Ex,
      joinType: String,
      direction: String,
      allowExactMatches: Boolean,
      tolerance: Option[Ex],
  )

  /** Lateral (correlated) join. */
  case LateralJoin(left: Rel, right: Rel, condition: Option[Ex], joinType: JoinType)

  /** Parse a single text column as CSV/JSON/XML; schema inferred server-side. */
  case Parse(input: Rel, format: ParseFormat, options: Map[String, String])

  /** Re-type the input to a declared schema — a typed mid-pipeline waypoint:
    * downstream inference resumes from these columns.
    */
  case ToSchema(input: Rel, schema: List[(String, ColType)])

  /** Empty local relation with a declared schema — the typed test/dimension
    * leaf. (Arrow-encoded *data* payloads are deliberately unsupported.)
    */
  case LocalRelation(schema: List[(String, ColType)])

  /** Inline literal table: a declared schema plus literal rows, row-major
    * (each inner list matches `schema` by position). The author surface is
    * `spark.createDataFrame(Seq(...)).toDF(...)`; the **encoder owns the wire
    * transport** and lowers this to SQL `VALUES` (D7) — Catalyst's
    * `EvalInlineTables` rebuilds the identical `LocalRelation` an Arrow payload
    * would, at zero capability cost for the small lookup/seed/enum tables this
    * targets. A build-time size guard ([[dev.sdp.core.InlineDataGuard]]) keeps
    * inline data small; larger data belongs in a source table.
    */
  case LocalData(schema: List[(String, ColType)], rows: List[List[LitValue]])

  /** Debug renders of the input (single string column, server-computed). */
  case ShowString(input: Rel, numRows: Int, truncate: Int, vertical: Boolean)
  case HtmlString(input: Rel, numRows: Int, truncate: Int)

  /** Table-valued function by unresolved name, e.g. `range`, `explode`. */
  case Tvf(functionName: String, args: List[Ex])

  /** Catalog queries as relations. */
  case Catalog(op: CatalogOp)

  /** Statistics relations — output schemas are computed server-side. */
  case Describe(input: Rel, cols: List[String])
  case Summary(input: Rel, statistics: List[String])
  case Crosstab(input: Rel, col1: String, col2: String)
  case Cov(input: Rel, col1: String, col2: String)
  case Corr(input: Rel, col1: String, col2: String)
  case FreqItems(input: Rel, cols: List[String])

enum JoinType:
  case Inner, FullOuter, LeftOuter, RightOuter, LeftAnti, LeftSemi, Cross

enum SetOpType:
  case Union, Intersect, Except

enum ParseFormat:
  // Xml exists in 4.2-dev protos but NOT the pinned 4.1.2 artifact
  case Csv, Json

enum CatalogOp:
  case CurrentDatabase, ListDatabases, ListTables

/** One sort key; nulls placement follows Spark defaults unless overridden. */
final case class SortKey(expr: Ex, descending: Boolean = false, nullsFirst: Option[Boolean] = None)

/** Tier-0 expressions: column references, literals, function application,
  * aliasing. `Fn` is deliberately *unresolved* — the server's analyzer binds
  * the name, exactly as it does for every other Spark client.
  */
enum Ex:
  case Col(name: String)
  case Lit(value: LitValue)
  case Fn(name: String, args: List[Ex], distinct: Boolean = false)
  case Alias(expr: Ex, name: String)

  /** ANSI cast to a declared column type. */
  case Cast(expr: Ex, to: ColType)

  /** SQL-fragment escape hatch inside expressions: `expr("id % 7")`. */
  case ExprString(sql: String)

  /** `*`, optionally qualified (`t.*`) — enables `count(*)`/`select(star)`. */
  case Star(target: Option[String])

  /** Container access: struct field, map key, array index. */
  case ExtractValue(child: Ex, extraction: Ex)

  /** Columns matched by regex (Spark `colRegex`). */
  case ColRegex(pattern: String)

  /** Call a registered function by exact name (vs `Fn`'s unresolved lookup). */
  case CallFn(name: String, args: List[Ex])

  /** Window function application: `fn over (partition by … order by …)`.
    * `frame = None` uses the server's default frame.
    */
  case Window(function: Ex, partitionBy: List[Ex], orderBy: List[SortKey], frame: Option[WindowFrame])

  /** A lambda for higher-order functions (`transform`, `filter`, ...).
    * Param names come from the author's actual Scala lambda; body references
    * them via [[LamVar]].
    */
  case Lam(params: List[String], body: Ex)

  /** A reference to an enclosing [[Lam]] parameter. */
  case LamVar(name: String)

  /** A relation in expression position. On the wire this lowers to a
    * `SubqueryExpression(plan_id)` with the relation shipped as a
    * `WithRelations` reference — the encoder does that bookkeeping; authors
    * just write `exists(...)`, `scalar(...)`, `.in(...)`.
    */
  case Subquery(rel: Rel, kind: SubqueryKind)

enum SubqueryKind:
  case Scalar
  case Exists
  /** `lhs IN (subquery)` — `values` are the left-hand expressions. */
  case In(values: List[Ex])

/** Window frame: rows/range between lower and upper boundaries. */
final case class WindowFrame(rowFrame: Boolean, lower: FrameBoundary, upper: FrameBoundary)

enum FrameBoundary:
  case CurrentRow
  case Unbounded
  case Value(expr: Ex)

enum LitValue:
  case Bool(value: Boolean)
  case I32(value: Int)
  case I64(value: Long)
  case F64(value: Double)
  case Str(value: String)
  case Null
