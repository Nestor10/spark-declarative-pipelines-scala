# Pipeline DSL (`sdp-runtime-dsl`)

> **Status: pre-release.** The DSL ships and is fully tested, but the sbt plugin
> that collects definitions at build time has not landed yet — today you invoke
> the assembly services directly (see [Validation](#validation)).

Declare Spark Declarative Pipelines datasets in pure Scala 3. Table names and
lineage are extracted **at compile time** by macros — invalid declarations fail
`compile`, not your cluster job.

## Declaring datasets

### `table` — batch table

```scala
import dev.sdp.dsl.*

val bronze = table("bronze_orders")
```

The name **must be a string literal**. A dynamic value is a compile error:

```scala
val name: String = computeName()
val bad = table(name)
// error: Table name must be a constant string literal so it can be
//        extracted at compile time; got: name
```

### `externalTable` — a source the pipeline reads but doesn't own

Real pipelines read tables that already exist in the catalog — bronze/raw
layers, shared dimensions. Declare them with `externalTable` so reads resolve
at build time without becoming pipeline-managed datasets:

```scala
val orders = externalTable("main.bronze.orders")   // exists in the catalog

val enriched = materializedView("orders_enriched") {
  spark.table("main.bronze.orders").where(col("amount") > lit(0L))
}
```

Reads of `main.bronze.orders` are **not** dangling references, and the server
resolves the table from the catalog at run time (an SDP *external input*) — it
is never registered as a managed dataset, so it gets no `DefineOutput` and no
flow. Typo-safety is preserved: only names you explicitly declare external are
treated as external, so a misspelled *managed* dataset name still fails the
build.

**The string is the catalog *location*, not a name you assign.** This matters,
and it's the opposite of the managed combinators:

| Combinator | What the string means |
|---|---|
| `materializedView("daily_orders")` (managed) | a **name you assign** — the pipeline *creates* this dataset; the name (qualified by the pipeline's target catalog/schema) becomes its identity |
| `externalTable("main.bronze.orders")` (external) | a **location that already exists** — you're *pointing at* it; the server resolves it as-is |

Why it can't be both: `externalTable(...)` itself sends nothing to the server;
the identifier reaches the analyzer through the **body** read
(`spark.table("main.bronze.orders")`), which must be the *same* string for the
lineage edge to connect. So whatever you pass is exactly what the catalog is
asked to resolve — use the **real, fully-qualified** identifier (Unity Catalog
`catalog.schema.table`), not a local alias. The name must be a string literal.

### `streamingTable` — streaming table with lineage

The body describes how the dataset reads from upstream datasets, written in
the same `spark.*` API you already use. Every `spark.table("...")` /
`spark.readStream.table("...")` reference becomes a lineage edge. The body is
**never executed during extraction** — only its shape is read.

```scala
val gold = streamingTable("gold_orders") {
  val stream = spark.readStream.table("silver_orders")
  val dims   = spark.read.table("dim_customers")
  stream.join(dims)(col("customer_id") === col("id"))
}
// gold's lineage: silver_orders -> gold_orders, dim_customers -> gold_orders
```

References are found through intermediate `val`s, joins, and nested
expressions. Repeated references to the same upstream dedupe to one edge.
Non-literal upstream names are compile errors, and **all** errors in a file are
reported in a single compile pass.

> The older `streamingTable("name") { ctx => ctx.readStream.table(...) }`
> context-style form and the `streamingTableFrom` spelling still compile, but
> the `spark.*` facade above is the documented surface — it's the exact
> SparkSession API, so existing Spark bodies extract unchanged.

### `materializedView` / `temporaryView` — SQL-backed datasets

```scala
val base    = temporaryView("base_numbers")("SELECT id FROM range(1, 100)")
val doubled = materializedView("doubled")("SELECT id * 2 AS double_id FROM base_numbers")
```

A materialized view is a batch dataset precomputed by exactly one SQL
transformation; a temporary view is ephemeral, scoped to a single run. Both
name and SQL must be string literals (each non-literal is its own compile
error — a bad call site reports both at once). On the server these resolve
fully: SQL-backed datasets are what make a pipeline pass `sdpPush`'s
dry-run validation today, ahead of the typed transformation algebra.

### `sqlStreamingTable` — streaming table with a SQL flow

```scala
val gold = sqlStreamingTable("gold_rates")(
  "SELECT value FROM STREAM(silver_rates) WHERE value > 0"
)
```

Declares a (streaming) table whose defining flow is a SQL query. SDP rule
learned from the live analyzer: every table on the server is a *streaming*
table, so the query must produce a streaming relation — read upstream
datasets with `STREAM(...)`. Note `STREAM` works over streaming tables, not
batch views; data enters a pipeline through real streaming sources (Kafka,
files, `rate`), which the typed algebra exposes as `Rel.DataSource`.

## The fluent flow language

`streamingTable` / `materializedView` also accept a **typed flow body** (in
place of a SQL string). The body is **extracted at compile time** — it never
executes; the macro interprets it into a relation tree that travels with your
build. It reads exactly like Spark:

```scala
import dev.sdp.dsl.*           // the spark.* facade, col/lit, operators
import dev.sdp.dsl.functions.* // count, sum, row_number, when, ... (optional)

val silver = streamingTable("silver_rates") {
  spark.readStream.format("rate").option("rowsPerSecond", "1").load().select(col("value"))
}

val gold = streamingTable("gold_rates") {
  val positive = spark.readStream.table("silver_rates").where(col("value") > lit(0L))
  positive.select(col("value"), (col("value") % lit(10L)).as("bucket"))
}

val daily = materializedView("value_counts") {
  spark.table("gold_rates").groupBy("value").count()
}
```

> This is the **SDP programming-guide example**, ported verbatim — the only
> change from the Python decorator form is that the dataset name is an explicit
> string argument (we extract names at compile time, not from the function
> name):
>
> ```scala
> val daily_orders_by_state = materializedView("daily_orders_by_state") {
>   spark.table("customer_orders")
>     .groupBy("state", "order_date")
>     .count()
>     .withColumnRenamed("count", "order_count")
> }
> ```

The language —

- **Leaves**: `spark.table(name)`, `spark.readStream.table(name)`,
  `spark.read.format(f).option(k, v).load()`,
  `spark.readStream.format(f).schema(ddl).load()`, `spark.range(start, end[, step])`,
  `spark.createDataFrame(Seq(...)).toDF(...)` (inline literal tables, below),
  `spark.sql("...")` (the SQL escape hatch)
- **Transformations**: `select` (columns or bare string names), `where` /
  `filter`, `groupBy(...).agg(...)` and `groupBy(...).count()`, `orderBy` /
  `sort`, `limit`, `offset`, `tail`, `distinct`, `dropDuplicates(cols*)`,
  `drop(cols*)`, `alias(name)`, `toDF(names*)`, `withColumn(name, expr)`,
  `withColumns(Map(name -> expr, ...))`, `withColumnRenamed(from, to)`,
  `withColumnsRenamed(Map(...))`, `sample(fraction[, seed])`,
  `hint(name, params*)`, `repartition(n)` / `coalesce(n)` /
  `repartitionBy(exprs*)`
- **Statistics & nulls** (Spark namespaces): `df.na.drop()` / `df.na.fill(lit)` /
  `df.na.replace(col, Map(old -> new))`; `df.stat.crosstab(c1, c2)` /
  `cov` / `corr` / `freqItems(Seq(cols))`; plus `describe(cols*)` /
  `summary(stats*)` (output schemas computed server-side)
- **Reshaping**: `unpivot(ids*)(varName, valName)` (melt),
  `transpose(indexCols*)`, `replaceValues(oldLit, newLit, cols*)`
- **Observability**: `observe(name, metrics*)` — a named metrics checkpoint
  in the flow (Spark `CollectMetrics`); data passes through untouched
- **Joins**: `join`, `joinLeft`, `joinRight`, `joinFull`, `joinSemi`,
  `joinAnti` (each `(other)(condition)`), `crossJoin(other)`
- **Set ops**: `union` (ALL semantics, like Spark), `intersect`, `except`,
  `intersectAll`, `exceptAll`
- **Expressions**: `col`, `lit`, the curated `functions.*` surface
  (`count`, `sum`, `avg`, `row_number`, `coalesce`, `to_date`, `concat_ws`,
  `when(c, v).otherwise(e)`, …) plus `fn(name, args*)` for *any* Spark SQL
  function (the server binds names — no client registry to go stale),
  operators (`+ - * / % > < >= <= === =!= && ||`), `.as`, `.asc`, `.desc`,
  `.cast(type)` (ANSI; cast-then-alias lands typed in generated schemas),
  `.isin(lits*)`, `star` (`select(star)`, `count(star)`), `expr("...")`
  (SQL-fragment escape hatch), `.getItem(key)` / `.getField(name)`
  (map/array/struct access)
- **Windows**: `row_number().over(Window.partitionBy(cols*).orderBy(keys*))`
  — ranking, lag/lead, running aggregates; custom frames available on the
  programmatic API (`Ex.Window` with `WindowFrame`)
- **Lambdas** — higher-order functions take *real Scala lambdas*, exactly as
  in Spark; your parameter name travels to the server:
  `transform(col("xs"), x => x * lit(2))`,
  `zip_with(col("a"), col("b"), (l, r) => l + r)`
- **Subqueries** — relations directly in expression position:
  `where(exists(spark.table("flags").where(...)))`,
  `select(scalar(spark.table("stats").agg(...)).as("max_v"))`,
  `col("id").in(spark.table("allowed_ids"))`. Lineage sees *through*
  subqueries — datasets they read become edges, cycles included. (On the
  wire these lower to plan-id references; you never see that.)

**`val` intermediates work** — name your steps. Consecutive `withColumn`
calls collapse into one operation. Anything outside the language is a
*positioned compile error* naming what it saw. Compile cost is negligible:
200 declarations extract in ~3 seconds.

### Inline tables (`spark.createDataFrame`)

Small lookup/seed/enum tables can live in the pipeline source — no external
table, no `sdpSeed`. Use Spark's own spelling:

```scala
val regions = materializedView("regions") {
  spark.createDataFrame(Seq(
    ("CA", "West"),
    ("NY", "East"),
    ("TX", "South"),
  )).toDF("state", "region")
}
```

The macro reads the literal rows and the `.toDF(...)` names at compile time
(the body never runs) and lowers them to SQL `VALUES` — Spark's analyzer
rebuilds the identical in-memory relation an Arrow upload would, with no
runtime data shipped from the build. Column types are inferred from the
literals: numeric columns widen (`Int` < `Long` < `Double`), a `null` cell
takes its column's type from the other rows, and each cell is `CAST` to its
type so the schema is exact. A single-column table uses bare values
(`Seq(1L, 2L, 3L)`); omit `.toDF(...)` to get `_1`, `_2`, … names.

This is for **small** tables: a build-time guard caps an inline table at 1000
rows / 64 KB (a *positioned compile error* otherwise). Larger data belongs in
a source table — `externalTable` + the catalog, or `sdpSeed`. Cells must be
literals (`Int`/`Long`/`Double`/`Boolean`/`String`/`null`); for richer shapes,
build the rows with a SQL `VALUES`/`SELECT` via `spark.sql(...)`.

### Schema checking (gradual)

Declare columns where they're unknowable — external sources — and every
downstream column reference is checked at build time:

```scala
val rawEvents = streamingTable("raw_events") {
  spark.readStream.format("rate")
    .schema("timestamp TIMESTAMP, value BIGINT")
    .load()
    .select(col("timestamp"), col("value").as("event_id"))
}
```

The `.schema("...")` string is Spark's DDL schema syntax — the same string
`DataFrameReader.schema` accepts — parsed at compile time. Schemas
*propagate*: `select`/`withColumn`/`drop`/`toDF`/renames/joins transform the
inferred column set, so a typo'd column **any number of datasets downstream**
fails `sdpManifest` with:

```
flow 'typo_orders' references unknown column 'amout'
  (available: timestamp, event_id, order_id, amount)
```

Checked DDL types: `BOOLEAN, INT, BIGINT, DOUBLE, STRING, TIMESTAMP, DATE`
(case-insensitive, with Spark's aliases — `INTEGER`, `LONG`). Other valid
Spark types (`DECIMAL(10,2)`, `ARRAY<…>`, `STRUCT<…>`) keep the column *name*
checked, type gradual. Checking is itself **gradual**: datasets defined by SQL
bodies (or sources without `.schema(...)`) have unknown shape and everything
downstream of them is simply unchecked — the server's dry-run remains the
final authority. Qualified references (`a.b`) are left to the server.

### Typed column references (`cols[S]`)

Describe a schema as a Scala 3 named tuple and get *type-checked,
IDE-autocompleted* column references — wrong names fail typing before
extraction even runs:

```scala
type Orders = (order_id: Long, amount: Long, customer_name: String)

val gold = streamingTable("gold") {
  val c = cols[Orders]
  spark.readStream.table("orders")
    .where(c.amount > lit(0L))          // c.amout would be a TYPE error
    .select(c.order_id, (c.amount * lit(2L)).as("doubled"))
}
```

`c.amount` extracts to exactly what `col("amount")` produces — the two styles
mix freely, and the typed layer adds no measurable compile cost (leaf-only by
design; no type-level schema propagation).

Write schema aliases by hand, or generate them: **`sbt sdpImportSchemas`**
emits aliases for every dataset whose shape the pipeline can infer, plus any
remote catalog tables listed in `sdpCatalogTables` — into a checked-in source
file, so schema drift shows up as a reviewable diff. Declared types survive
inference (a renamed `timestamp` column stays `java.sql.Timestamp` in the
generated alias).

Two payoffs over SQL strings:

1. **Lineage is derived from the body** — `spark.readStream.table("silver_rates")`
   creates the edge; cycles and dangling references across flow bodies fail
   at `compile`, not at submission.
2. **Function names are unresolved on the wire** — the server's analyzer
   binds them, so the whole Spark SQL function library is available via the
   `functions.*` surface or `fn("name", args*)`.

## Validation

Each declaration produces a `GraphFragment`. Fragments merge order-independently
and validate as one graph — duplicates, dangling references, and cycles are all
reported together:

```scala
import dev.sdp.app.{GraphValidation, ManifestAssembly}

val program = ManifestAssembly.assemble(List(bronze, silver, gold))
  .provide(ManifestAssembly.live, GraphValidation.live)
// IO[::[PipelineValidationError], PipelineManifest]
```

Validation errors (`PipelineValidationError`):

| Error | Meaning |
|---|---|
| `DuplicateNode(id)` | Two declarations share a dataset id (e.g. across modules) |
| `DanglingEdges(ids)` | Lineage references datasets nobody declares |
| `CycleDetected(path)` | Circular lineage, reported as a closed path: `List("a", "b", "a")` |

The error channel is `::[PipelineValidationError]` (a non-empty list): a
failure provably carries at least one error, and *all* problems surface in one
pass.

## The manifest

A valid graph renders to a canonical, byte-stable manifest (`sdp-manifest/1`):
nodes sorted by id, edges sorted by endpoints, fields percent-encoded, no
timestamps. Equal pipelines always produce identical bytes — this is what makes
build caching and change detection reliable.

```
sdp-manifest/1
node|bronze_orders|table|delta
node|dim_customers|table|delta
node|gold_orders|streaming-table|delta
node|silver_orders|streaming-table|delta
edge|bronze_orders|silver_orders
edge|dim_customers|gold_orders
edge|silver_orders|gold_orders
```

`PipelineManifest.parse` is the inverse of `render` (round-trip law, tested).

## Supported Spark surface

Every relation and expression the DSL emits is verified against a live Spark
server's analyzer before being claimed. Current coverage (regenerated by the
build's conformance report, which also gates against upstream Spark changes):

- **Core relational algebra: 100%** — reads/sources, project, filter, all 7
  join types, aggregate, set ops, sort/limit/offset/tail, dedup, drop,
  withColumn(s)/renames/toDF, range, SQL escape hatch
- **Extended: 100%** — sampling (incl. stratified), hints, repartitioning,
  NA handling (drop/fill/replace), statistics (describe/summary/crosstab/
  cov/corr/quantiles/freqItems), reshaping (unpivot/transpose), observe
  metrics, as-of & lateral joins, CSV/JSON parse, typed schema waypoints
  (`toSchema`), schema-only local relations, table-valued functions, catalog
  queries (some specialty shapes are programmatic-API only — `Rel`/`Ex`
  trees — rather than fluent combinators)
- **Expressions** — columns, literals, the *entire* Spark SQL function
  library via `fn(name, args*)` (names bind server-side; no client registry
  to go stale), operators, aliases, sort keys, ANSI `cast`

Anything not yet covered is reachable through the SQL escape hatch
(`sqlStreamingTable`, `materializedView`), and unsupported constructs in flow
bodies are *positioned compile errors* that name the supported language —
nothing is silently dropped.
