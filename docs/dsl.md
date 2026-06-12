# Pipeline DSL (`dev.sdp.dsl`, in the `sdp` library)

Declare Spark Declarative Pipelines datasets in pure Scala 3. The DSL is a
**runtime plan-builder**: each combinator is an ordinary function that builds a
`GraphFragment` value. You list the fragments in one `Pipeline(...)` and the sbt
plugin (or the `SdpApp` runner) assembles and validates the graph — dangling
references and cycles fail `sbt sdpValidate`/`sdpManifest`, not your cluster job.

Because it's plain Scala, **the whole host language is available** inside a flow
body and around your declarations: helper functions, loops, conditionals,
collections — none of the literal-only restrictions an earlier macro frontend
imposed (see [Design history](#design-history)).

## Declaring datasets

### `table` — batch table

```scala
import dev.sdp.dsl.*

val bronze = table("bronze_orders")               // declared managed table
val cleaned = table("orders_clean") {              // table backed by a flow
  spark.table("bronze_orders").where(col("amount") > lit(0L))
}
```

The name is just a `String` — compute it however you like:

```scala
def bucket(n: Int) = table(s"bucket_$n")           // perfectly fine now
val buckets = (0 until 4).map(bucket).toList
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
treated as external, so a misspelled *managed* dataset name still fails
`sdpValidate`/`sdpManifest`.

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
`catalog.schema.table`), not a local alias.

### `streamingTable` — streaming table with lineage

The body describes how the dataset reads from upstream datasets, written in
the same `spark.*` API you already use. Every `spark.table("...")` /
`spark.readStream.table("...")` reference becomes a lineage edge. The body
**runs** at assembly and builds a relation tree — it never touches a cluster
(the builder records the plan, it doesn't execute Spark).

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
Because the body is ordinary Scala, you can factor it into helpers and reuse
them across datasets.

> The `streamingTableFrom` spelling is an alias for `streamingTable` (muscle
> memory). The `spark.*` facade is the documented surface — it's the exact
> SparkSession API, so existing Spark bodies port across unchanged.

### `materializedView` / `temporaryView` — SQL-backed datasets

```scala
val base    = temporaryView("base_numbers")("SELECT id FROM range(1, 100)")
val doubled = materializedView("doubled")("SELECT id * 2 AS double_id FROM base_numbers")
```

A materialized view is a batch dataset precomputed by exactly one SQL
transformation; a temporary view is ephemeral, scoped to a single run. On the
server these resolve fully: SQL-backed datasets are what make a pipeline pass
`sdpPush`'s dry-run validation today, ahead of the typed transformation algebra.

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

### AUTO CDC (Spark 4.2, gated)

Spark 4.2 adds **AUTO CDC** to SDP — the declarative MERGE/SCD construct
donated from DLT's `apply_changes`. You declare a *streaming-table shell* as
the target and an `createAutoCdcFlow` that streams a CDC source into it:

```scala
val cdcSource = externalTable("bronze.customer_cdc")
val dim       = createStreamingTable("dim_customers")           // body-less target shell

val applyCdc = createAutoCdcFlow(
  target         = "dim_customers",
  source         = "bronze.customer_cdc",
  keys           = Seq(col("id")),                 // row identity (Seq[Column] or Seq[String])
  sequenceBy     = col("event_ts"),                // ordering of source events (Column or String)
  applyAsDeletes = Some(col("op") === lit("DELETE")),
  storedAsScdType = 1,                             // only SCD type 1 exists so far
)

object Customers extends SdpApp:
  def pipeline = Pipeline(cdcSource, dim, applyCdc)
```

Surface (mirrors the official Python `create_auto_cdc_flow`):
`createAutoCdcFlow(target, source, keys, sequenceBy, applyAsDeletes?,
applyAsTruncates?, columnList?, exceptColumnList?,
ignoreNullUpdatesColumnList?, ignoreNullUpdatesExceptColumnList?,
storedAsScdType = 1, name = Some(s"${target}_auto_cdc"), once = false)`.
A `Seq[String]`/`String` overload of `keys`/`sequenceBy` lowers to `col(...)`.

**Semantics.** The flow MERGEs the source into the target; the `source`
becomes a read (lineage edge) of the flow, so a missing source is caught by
the dangling-dependency validator. Validation also enforces that the target is
a declared **streaming table** and that `keys` is non-empty.

**Gated at the wire.** Our wire client pins `spark-connect-common 4.1.2`,
which has no `AutoCdcFlowDetails` message. So `sdpValidate` / `sdpManifest`
(offline, deterministic) fully support AUTO CDC *today* — the domain, codec,
and validation all work — but `sdpRun` / `sdpDryRun` (and `SdpApp run`) fail
with a clear error until the dep bumps to `spark-connect-common >= 4.2.0`:

> AUTO CDC flow '…' requires a Spark 4.2+ wire client
> (spark-connect-common >= 4.2.0); this build pins 4.1.2 — validate/manifest
> work, run/dry-run cannot register this flow yet.

The manifest header bumps to `sdp-manifest/3` when a pipeline contains an AUTO
CDC flow (or a `once` flow, below); pipelines without these constructs still
write `sdp-manifest/2`, byte-identical to before.

### One-time / backfill flows (`once`)

`streamingTableOnce(name)(body)` / `tableOnce(name)(body)` declare a flow whose
body is a batch DataFrame that runs **once** and re-runs only on full refresh
(proto `DefineFlow.once`). `createAutoCdcFlow(..., once = true)` does the same
for an AUTO CDC flow. Like AUTO CDC, a `once = true` flow renders under
`sdp-manifest/3`.

## The fluent flow language

`streamingTable` / `materializedView` also accept a **typed flow body** (in
place of a SQL string). The body **runs** when the pipeline is assembled and
builds a relation tree that travels with your build — it reads exactly like
Spark, but the builder records the plan rather than executing it:

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
> string argument (you name the dataset, not the enclosing function):
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
calls collapse into one operation. And since a flow body is ordinary Scala,
you can build it with helpers, loops, and conditionals:

```scala
val report = materializedView("report") {
  // a host-language loop the macro world couldn't express
  val metricCols = Seq("clicks", "views", "signups").map(m => sum(col(m)).as(m))
  spark.table("events").groupBy(col("day")).agg(metricCols*)
}
```

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

The builder reads the rows and the `.toDF(...)` names and lowers them to SQL
`VALUES` — Spark's analyzer rebuilds the identical in-memory relation an Arrow
upload would, with no data shipped from the build. Column types are inferred
from the literals: numeric columns widen (`Int` < `Long` < `Double`), a `null`
cell takes its column's type from the other rows, and each cell is `CAST` to its
type so the schema is exact. A single-column table uses bare values
(`Seq(1L, 2L, 3L)`); omit `.toDF(...)` to get `_1`, `_2`, … names.

This is for **small** tables: an assembly-time guard caps an inline table at
1000 rows / 64 KB (the assembly fails otherwise). Larger data belongs in a
source table — `externalTable` + the catalog, or `sdpSeed`. Cells are
`Int`/`Long`/`Double`/`Boolean`/`String`/`null`; for richer shapes, build the
rows with a SQL `VALUES`/`SELECT` via `spark.sql(...)`.

### Schema checking (gradual)

Declare columns where they're unknowable — external sources — and every
downstream column reference is checked when the pipeline is assembled
(`sdpValidate`/`sdpManifest`):

```scala
val rawEvents = streamingTable("raw_events") {
  spark.readStream.format("rate")
    .schema("timestamp TIMESTAMP, value BIGINT")
    .load()
    .select(col("timestamp"), col("value").as("event_id"))
}
```

The `.schema("...")` string is Spark's DDL schema syntax — the same string
`DataFrameReader.schema` accepts — parsed when the body runs. Schemas
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
IDE-autocompleted* column references — wrong names are a **type error** at
`compile`, before assembly even runs (this is the one small `inline` kept in
the runtime DSL):

```scala
type Orders = (order_id: Long, amount: Long, customer_name: String)

val gold = streamingTable("gold") {
  val c = cols[Orders]
  spark.readStream.table("orders")
    .where(c.amount > lit(0L))          // c.amout would be a TYPE error
    .select(c.order_id, (c.amount * lit(2L)).as("doubled"))
}
```

`c.amount` builds exactly what `col("amount")` produces — the two styles mix
freely, and the typed layer adds no measurable compile cost (leaf-only by
design; no type-level schema propagation).

Write schema aliases by hand, or generate them: **`sbt sdpImportSchemas`**
emits aliases for every dataset whose shape the pipeline can infer, plus any
remote catalog tables listed in `sdpCatalogTables` — into a checked-in source
file, so schema drift shows up as a reviewable diff. Declared types survive
inference (a renamed `timestamp` column stays `java.sql.Timestamp` in the
generated alias).

Two payoffs over SQL strings:

1. **Lineage is derived from the body** — `spark.readStream.table("silver_rates")`
   creates the edge; cycles and dangling references across flow bodies fail at
   `sdpValidate`/`sdpManifest`, not at submission.
2. **Function names are unresolved on the wire** — the server's analyzer
   binds them, so the whole Spark SQL function library is available via the
   `functions.*` surface or `fn("name", args*)`.

## Assembling a pipeline — `Pipeline` and `SdpApp`

Each combinator returns a `GraphFragment`. You collect every dataset into one
list with `Pipeline(...)` (just `List[GraphFragment]`) and expose it from a
single object the tooling can find:

```scala
import dev.sdp.dsl.*
import dev.sdp.connect.app.SdpApp

object Warehouse extends SdpApp:
  def pipeline = Pipeline(
    bronzeOrders,
    silverOrders,
    goldOrders,
  )
```

`SdpApp` (from `dev.sdp.connect.app`) makes that same object a **standalone runner**:
the uber jar IS the production entry point — `java -jar app.jar validate`,
`manifest [--out p]`, or `run [--dry]`, with config read from the environment
(`SDP_CONNECT_ENDPOINT`, `SDP_STORAGE_ROOT`, `SDP_PIPELINE_NAME`, and the
dev/prod switch `SDP_DEFAULT_CATALOG`/`SDP_DEFAULT_DATABASE` — run-only,
unset = omit). Argo/K8s
schedules the jar with no sbt on the path. The sbt plugin
([plugin.md](plugin.md)) drives the *same* `pipeline` value for the dev loop via
`sdpPipelineClass := "com.example.Warehouse"`.

You don't have to extend `SdpApp` — any object with `def pipeline:
List[GraphFragment]` works for the plugin; `SdpApp` just adds the runner
subcommands.

## Validation

Fragments merge order-independently and validate as one graph — duplicates,
dangling references, and cycles are all reported together. Under the hood both
the plugin and `SdpApp` call the same assembly service:

```scala
import dev.sdp.app.{GraphValidation, ManifestAssembly}

val program = ManifestAssembly.assemble(Pipeline(bronze, silver, gold))
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

A valid graph renders to a canonical, byte-stable manifest (`sdp-manifest/2`):
nodes sorted by id, edges sorted by endpoints, flows sorted by (target, name),
fields percent-encoded, no timestamps. Equal pipelines always produce identical
bytes — this is what makes build caching and change detection reliable.

```
sdp-manifest/2
node|bronze_orders|table|delta
node|dim_customers|table|delta
node|gold_orders|streaming-table|delta
node|silver_orders|streaming-table|delta
edge|bronze_orders|silver_orders
edge|dim_customers|gold_orders
edge|silver_orders|gold_orders
flow|gold_orders|gold_orders|<canonical relation tree>
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
(`sqlStreamingTable`, `materializedView`). Because the builder is ordinary
Scala, a construct it doesn't model is simply a method that doesn't exist —
the compiler tells you at the call site.

## Design history

An earlier version of this DSL was a **compile-time macro frontend**:
`transparent inline` entry points whose bodies were walked by `quotes.reflect`
AST extraction, with the resulting lineage embedded into each call site's TASTy
as a string constant for the sbt plugin to scan. That bought compile-time
extraction but cost the host language — flow bodies could only contain the
shapes the macro recognized, and arguments had to be string literals.

Decision **D10** replaced it with the runtime plan-builder documented here. The
builder produces the identical algebra trees (a frozen render-equivalence
oracle guards that), but bodies are now plain `def`s and values — so loops,
helpers, and conditionals all work. The plugin discovers fragments by
*evaluating* your pipeline object in an isolated classloader and reading the
fragment strings back across the boundary (the same string contract TASTy used);
there is no macro, no TASTy scan, and no `inline` except the small `cols[S]`
type-level field check.
