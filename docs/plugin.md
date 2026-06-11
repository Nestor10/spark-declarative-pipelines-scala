# The sbt plugin (`sbt-spark-pipelines`)

> **Status: pre-release.** Manifest generation and Spark Connect push are both
> functional and tested end-to-end against Spark 4.1.2.

Generates a validated pipeline manifest from your compiled Scala sources at
build time. Invalid graphs **fail the build** — cycles, duplicates, and
dangling references never reach a cluster.

## Setup

```scala
// project/plugins.sbt
addSbtPlugin("dev.sdp" % "sbt-spark-pipelines" % <version>)

// build.sbt
lazy val myPipelines = (project in file("."))
  .enablePlugins(dev.sdp.plugin.SparkPipelinesPlugin)
  .settings(
    scalaVersion := "3.8.4",
    libraryDependencies += "dev.sdp" %% "sdp-runtime-dsl" % <version>,
  )
```

The plugin does not auto-activate (`noTrigger`); enable it per project.

## Tasks and settings

| Key | Type | Purpose |
|---|---|---|
| `sdpManifest` | task | Compile, discover pipeline fragments, validate the DAG, write `<target>/sdp/pipeline.sdpm` |
| `sdpPush` | task | Register the graph with the remote `PipelinesHandler`; validate (dry) or run per `sdpDryRun` |
| `sdpRun` | task | Register **and execute** the graph (dry = false, always) — materializes tables; one-shot run with progress |
| `sdpWatch` | task | Re-trigger the pipeline every `sdpWatchInterval`s (Ctrl-C to stop) — client-side "continuous": each cycle is a triggered run whose AvailableNow resumes from the checkpoint and picks up new data (the server has no true continuous mode) |
| `sdpSeed` | task | Run `sdpSeedStatements` (DDL/DML) against the server over Spark Connect — a local fixture to create + populate the source/catalog tables an `externalTable` reads, so a full run resolves them |
| `sdpSeedStatements` | setting | SQL statements `sdpSeed` executes (e.g. `CREATE OR REPLACE TABLE bronze.orders USING delta AS SELECT …`). Default empty |
| `sdpConnectEndpoint` | setting | gRPC endpoint, `sc://host:port` (default `sc://localhost:15002`) |
| `sdpStorageRoot` | setting | Checkpoint/metadata root — absolute URI with scheme (default `file:///tmp/sdp/<project>`) |
| `sdpDryRun` | setting | `true` (default): server validates only, no flows execute; `false`: really run |
| `sdpImportSchemas` | task | Generate named-tuple schema aliases (for `cols[S]`) from the pipeline's inferred shapes + remote catalog tables |
| `sdpSchemasFile` | setting | Output for generated aliases (default `src/main/scala/sdp/schemas/PipelineSchemas.scala` — checked in, diffs reviewable) |
| `sdpSchemasPackage` | setting | Package of the generated aliases (default `sdp.schemas`) |
| `sdpCatalogTables` | setting | Remote tables to import (schema via the server's analyzer), e.g. `Seq("sales.orders")` |

## What `sdpPush` does

Reads the manifest from `sdpManifest`, then drives the registration
sequence over gRPC: `CreateDataflowGraph` → one `DefineOutput`/`DefineFlow`
per dataset → `StartRun`. With `sdpDryRun := true` the server's Catalyst
analyzer fully resolves and validates the graph without executing anything:

```
[info] sdp: pushing 3 dataset(s) to sc://localhost:15002 (dry=true, storage=file:///tmp/sdp/demo)
[info] sdp: pipeline validated (dry run) on the server; dataflow graph id: 8b744454-...
```

Failures are rendered by kind: an unreachable server reports as a transport
failure; an analyzer rejection (unresolvable dataset, invalid flow type)
reports the server's own diagnostic.

## Running for real — `sdpRun`

`sdpPush` defaults to dry (validate only). To actually materialize
tables, use `sdpRun` (always `dry = false` — a dedicated task rather
than flipping `sdpDryRun`, which avoids an sbt thin-client `set` quirk). The
server runs a *triggered* execution: batch datasets compute once and the run
terminates, with flow progress surfaced live:

```
[info] sdp: running 1 dataset(s) on sc://localhost:15002 (dry=false, storage=file:///data/sdp-storage)
[info] sdp:   • Flow spark_catalog.default.nums_run is QUEUED.
[info] sdp:   • Flow spark_catalog.default.nums_run is PLANNING → STARTING → RUNNING.
[info] sdp:   • Flow spark_catalog.default.nums_run has COMPLETED.
[info] sdp:   • Run is COMPLETED.
[info] sdp: pipeline executed on the server; dataflow graph id: 0540ddba-...
```

Progress is parsed into a structured form (`dev.sdp.core.RunProgress`) and
surfaced **live** as each event arrives — the same hook a future DAG view would
render from.

**Streaming runs terminate too.** The server runs a *triggered* execution with
`Trigger.AvailableNow`, so a streaming flow (Delta source, file source, even
`rate`) processes the data available at trigger time and completes — not just
batch materialized views. `sdpRunTimeout` (default 600s) bounds the run: a
genuinely never-ending pipeline detaches gracefully instead of wedging the
build, rather than blocking forever. File/CSV/JSON streaming sources work
natively — declare `.schema("id BIGINT, v STRING")` and it's emitted to the
server (the raw DDL, verbatim, so `DECIMAL`/`ARRAY`/`STRUCT` survive intact);
Delta and `rate` self-describe and need no schema.

A [Spark Connect + Delta server](../sdp-example/docker-compose.yml) is enough to
materialize real Delta tables locally.

**Re-runs and `sdpWatch` are safe.** Running the same pipeline again (or each
`sdpWatch` cycle) re-materializes existing tables — materialized views
full-refresh, streaming tables resume from the checkpoint — no manual drop
required. The plugin never puts a table's provider on the wire for the default
format (it lets the catalog default decide, exactly as the official SDP does),
because the server re-asserts every table property on a re-run's `ALTER` and a
provider can't be re-altered.

**Two version facts make local re-runs work** (both are about the *example
container*, not the plugin):

- The container is Spark **4.1.1**, not 4.1.2 — 4.1.2 removed a Catalyst class
  (`IgnoreCachedData`) that Delta 4.2.0, built against Spark 4.1.0, needs on the
  re-run path.
- The container's default provider is **parquet**, not Delta. SDP full-refreshes
  a materialized view with `TRUNCATE TABLE`, which **OSS Delta 4.2.0 does not
  support** (it lands `Table does not support truncates`). SDP's own test
  harness defaults to parquet for the same reason. Delta stays loaded for
  reading Delta sources and explicit `USING delta`. Truncate support arrives in
  **Delta 4.3** ([delta-io/delta#6845](https://github.com/delta-io/delta/pull/6845));
  when 4.3 ships, bump the Delta jars and flip `spark.sql.sources.default` back
  to `delta` — no plugin change needed, since the format comes from the catalog
  default.

## What `sdpManifest` does

1. Compiles the project (if needed).
2. Scans the compiled `.tasty` files for fragments embedded by the
   [DSL macros](dsl.md) — **without classloading or executing your code**.
3. Merges all fragments, validates the full graph on an isolated ZIO runtime.
4. Writes the canonical manifest, or fails the build listing **every** problem:

```
[error] (sdpManifest) SDP pipeline graph is invalid:
[error]   - cyclic dependency: table_a -> table_b -> table_a
[error]   - lineage references undeclared dataset(s): typo_orders
```

## Behavior guarantees (all covered by scripted tests)

- **Incremental correctness.** Deleting or renaming a source file removes its
  datasets from the next manifest — no `clean` ever required. Discovery rides
  on `.tasty` files, whose lifecycle Zinc owns.
- **Declaration-based discovery.** A dataset declared inside a method that is
  never called is still part of the graph. Declarative semantics: writing the
  declaration registers it.
- **Cache-clean.** The task participates in sbt 2.0's action cache: unchanged
  inputs replay from cache (`cache 100%`) with byte-identical output;
  changing any compiled source invalidates precisely.
- **Build-JVM hygiene.** No classloaders are created over project code, and
  the ZIO runtime is task-scoped and torn down per invocation — safe for
  long-lived sbt servers.

## The manifest artifact

`<target>/sdp/pipeline.sdpm`, format `sdp-manifest/1` — canonical and
byte-stable (sorted entries, percent-encoded fields, no timestamps). See
[the DSL doc](dsl.md#the-manifest) for the format itself.
