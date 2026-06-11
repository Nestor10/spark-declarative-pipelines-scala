# The sbt plugin (`sbt-spark-pipelines`)

> **Status: pre-release.** Manifest generation and Spark Connect push are both
> functional and tested end-to-end against Spark 4.1.2.

Generates a validated pipeline manifest from your pipeline object. Invalid
graphs **fail the build** — cycles, duplicates, and dangling references never
reach a cluster. The plugin is a dev-loop convenience over the same code the
`SdpApp` uber-jar runner uses in production (D10).

## Setup

```scala
// project/plugins.sbt
addSbtPlugin("dev.sdp" % "sbt-spark-pipelines" % <version>)

// build.sbt
lazy val myPipelines = (project in file("."))
  .enablePlugins(dev.sdp.plugin.SparkPipelinesPlugin)
  .settings(
    scalaVersion     := "3.8.4",
    // The object that exposes `def pipeline: List[GraphFragment]`
    // (typically `object X extends SdpApp`). The plugin loads it and calls it.
    sdpPipelineClass := "com.example.Warehouse",
  )
```

You write **one** version (the `addSbtPlugin` line). The matching
`sdp-runtime-dsl` (the authoring surface) and `sdp-connect` (`SdpApp` + the
Connect client) are injected automatically, in lockstep with the plugin —
override with `sdpRuntimeVersion` only for local testing. The plugin does not
auto-activate (`noTrigger`); enable it per project.

See [the DSL doc](dsl.md#assembling-a-pipeline--pipeline-and-sdpapp) for how to
write the pipeline object.

## Tasks and settings

| Key | Type | Purpose |
|---|---|---|
| `sdpPipelineClass` | setting | FQN of your pipeline object (`object X extends SdpApp`, or any object with `def pipeline: List[GraphFragment]`). **Required.** |
| `sdpManifest` | task | Evaluate the pipeline object, validate the DAG, write `<target>/sdp/pipeline.sdpm` |
| `sdpValidate` | task | Assemble + validate, print the verdict — **no file output**. The offline inner-loop target for `~sdpValidate` |
| `sdpDryRun` | task | Register the graph server-side in validate-only mode (dry run). Target for `~sdpDryRun` |
| `sdpPush` | task | Register the graph with the remote `PipelinesHandler`; validate (dry) or run per `sdpPushDryRun` |
| `sdpRun` | task | Register **and execute** the graph (dry = false, always) — materializes tables; one-shot run with progress |
| `sdpWatch` | task | Re-trigger the pipeline every `sdpWatchInterval`s (Ctrl-C to stop) — client-side "continuous": each cycle is a triggered run whose AvailableNow resumes from the checkpoint and picks up new data (the server has no true continuous mode) |
| `sdpSeed` | task | Run `sdpSeedStatements` (DDL/DML) against the server over Spark Connect — a local fixture to create + populate the source/catalog tables an `externalTable` reads, so a full run resolves them |
| `sdpSeedStatements` | setting | SQL statements `sdpSeed` executes (e.g. `CREATE OR REPLACE TABLE bronze.orders USING delta AS SELECT …`). Default empty |
| `sdpConnectEndpoint` | setting | gRPC endpoint, `sc://host:port` (default `sc://localhost:15002`) |
| `sdpStorageRoot` | setting | Checkpoint/metadata root — absolute URI with scheme (default `file:///tmp/sdp/<project>`) |
| `sdpPushDryRun` | setting | `true` (default): `sdpPush` validates only, no flows execute; `false`: `sdpPush` really runs. (`sdpDryRun` always runs dry; `sdpRun` always runs for real.) |
| `sdpImportSchemas` | task | Generate named-tuple schema aliases (for `cols[S]`) from the pipeline's inferred shapes + remote catalog tables |
| `sdpSchemasFile` | setting | Output for generated aliases (default `src/main/scala/sdp/schemas/PipelineSchemas.scala` — checked in, diffs reviewable) |
| `sdpSchemasPackage` | setting | Package of the generated aliases (default `sdp.schemas`) |
| `sdpCatalogTables` | setting | Remote tables to import (schema via the server's analyzer), e.g. `Seq("sales.orders")` |

## The inner loop — `~sdpValidate` / `~sdpDryRun`

For fast feedback while authoring, run a validate task under sbt's file watch:

```
sbt:myPipelines> ~sdpValidate
```

`sdpValidate` evaluates the pipeline object, assembles + validates the graph,
and prints a one-line verdict — **no file is written, no server is contacted**.
Every save re-runs it, so a cycle or dangling reference shows up in the terminal
within seconds. It's the offline equivalent of `app.jar validate`.

When you also want the server's analyzer to vet the graph each save, watch the
dry run instead:

```
sbt:myPipelines> ~sdpDryRun
```

`sdpDryRun` builds the manifest and registers the graph server-side in
validate-only mode (it never materializes tables) — catching anything only the
live Catalyst analyzer knows (unresolvable functions, type mismatches).

## What `sdpPush` does

Reads the manifest from `sdpManifest`, then drives the registration
sequence over gRPC: `CreateDataflowGraph` → one `DefineOutput`/`DefineFlow`
per dataset → `StartRun`. With `sdpPushDryRun := true` (the default) the server's
Catalyst analyzer fully resolves and validates the graph without executing
anything:

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
than flipping `sdpPushDryRun`, which avoids an sbt thin-client `set` quirk). The
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

1. Compiles the project (it depends on `Runtime / fullClasspath`).
2. **Classload-eval** (D10): builds an isolated, child-first `URLClassLoader`
   over the project's runtime classpath, loads `sdpPipelineClass`, calls
   `pipeline`, and renders each fragment to a STRING via
   `dev.sdp.core.PipelineExport.encodeAll`. The fragment string is the *only*
   thing that crosses the classloader boundary — the same contract the earlier
   TASTy embedding used — so the loader is fully isolated and `close()`d in a
   `finally`. (No macro, no TASTy scan, no `inline`.)
3. Decodes the strings, merges all fragments, validates the full graph on an
   isolated ZIO runtime.
4. Writes the canonical manifest, or fails the build listing **every** problem:

```
[error] (sdpManifest) SDP pipeline graph is invalid:
[error]   - cyclic dependency: table_a -> table_b -> table_a
[error]   - lineage references undeclared dataset(s): typo_orders
```

## Behavior guarantees (all covered by scripted tests)

- **Incremental correctness.** Editing the pipeline (adding or dropping a
  dataset) reshapes the next manifest with no `clean` ever required — the cache
  is keyed on the runtime classpath (compiled products + deps), so any source
  change invalidates precisely.
- **Single source of truth.** The graph is exactly what `pipeline` returns —
  you list every dataset explicitly, so what runs in the dev loop is what the
  `SdpApp` uber jar runs in production (the plugin and the jar evaluate the
  same `pipeline` value).
- **Cache-clean.** The task participates in sbt 2.0's action cache: unchanged
  inputs replay from cache (`cache 100%`) with byte-identical output.
- **Build-JVM hygiene.** The child classloader is closed per invocation and the
  ZIO runtime is task-scoped and torn down per invocation — safe for long-lived
  sbt servers. No ZIO runs inside the child loader; evaluation is plain code.

## The manifest artifact

`<target>/sdp/pipeline.sdpm`, format `sdp-manifest/2` — canonical and
byte-stable (sorted entries, percent-encoded fields, no timestamps). See
[the DSL doc](dsl.md#the-manifest) for the format itself.
