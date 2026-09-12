# The sbt plugin (`sbt-spark-pipelines`)

> **Status:** manifest generation, dry-run and real runs are all functional and
> tested end-to-end against a Spark **4.1.1** Connect server (the conformance
> oracle pins 4.1.2 for *analysis* only — see "Two version facts" below).

Generates a validated pipeline manifest from your pipeline object. Invalid
graphs **fail the build** — cycles, duplicates, and dangling references never
reach a cluster. The plugin is a dev-loop convenience over the same code the
`SdpApp` uber-jar runner uses in production (D10).

## Setup

```scala
// project/plugins.sbt
addSbtPlugin("io.github.nestor10" % "sbt-spark-pipelines" % <version>)

// build.sbt
lazy val myPipelines = (project in file("."))
  .enablePlugins(dev.sdp.plugin.SparkPipelinesPlugin)
  .settings(
    scalaVersion     := "3.9.0",
    // The object that exposes `def pipeline: List[GraphFragment]`
    // (typically `object X extends SdpApp`). The plugin loads it and calls it.
    sdpPipelineClass := "com.example.Warehouse",
  )
```

You write **one** version (the `addSbtPlugin` line): the matching `sdp` library
(authoring surface + `SdpApp` + the Connect client) is added to
`libraryDependencies` automatically, in lockstep with the plugin — override with
`sdpRuntimeVersion` only for local testing. The plugin does not
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
| `sdpConnectUseTls` | setting | `true` to speak TLS (default `false` = plaintext — `sc://localhost` is the dev container) |
| `sdpConnectToken` | setting | Bearer token attached to every call (`Authorization: Bearer …`). Defaults to the `SDP_CONNECT_TOKEN` env var so the secret stays out of `build.sbt`; `""` = anonymous. Never logged |
| `sdpConnectDeadline` | setting | Per-RPC deadline in seconds for registration/seed/analyze calls (default 60) so a wedged server can't hang the build. The run stream is bounded by `sdpRunTimeout` instead |
| `sdpStorageRoot` | setting | Checkpoint/metadata root — absolute URI with scheme (default `file:///tmp/sdp/<project>`) |
| `sdpDefaultCatalog` | setting | Graph default catalog sent in `CreateDataflowGraph` (`""` = omit). Send it on named V2 catalogs — omission can silently drop dependency edges |
| `sdpDefaultDatabase` | setting | Graph default database — the dev/prod switch (see "Environments" below; `""` = omit) |
| `sdpPushDryRun` | setting | `true` (default): `sdpPush` validates only, no flows execute; `false`: `sdpPush` really runs. (`sdpDryRun` always runs dry; `sdpRun` always runs for real.) |
| `sdpVersionCheck` | setting | `true` (default): ask the server which Spark it is before registering, and refuse constructs newer than it — see "Server-version handshake" below |
| `sdpImportSchemas` | task | Generate named-tuple schema aliases (for `cols[S]`) from the pipeline's inferred shapes + remote catalog tables |
| `sdpSchemasFile` | setting | Output for generated aliases (default `src/main/scala/sdp/schemas/PipelineSchemas.scala` — checked in, diffs reviewable) |
| `sdpSchemasPackage` | setting | Package of the generated aliases (default `sdp.schemas`) |
| `sdpCatalogTables` | setting | Remote tables to import (schema via the server's analyzer), e.g. `Seq("sales.orders")` |

> **AUTO CDC (Spark 4.2, gated):** `sdpValidate` / `sdpManifest` fully support
> pipelines containing `createAutoCdcFlow` today (offline). `sdpRun` /
> `sdpDryRun` fail with a clear error on such a pipeline because this build does
> not emit `AutoCdcFlowDetails` on the wire yet — see `docs/dsl.md`.

## Server-version handshake

Every task that talks to a server (`sdpDryRun`, `sdpRun`, `sdpPush`, `sdpWatch`,
and `SdpApp run` off sbt) asks it one question first — a single
`AnalyzePlan`/`SparkVersion` round trip on the same channel the registration
uses — and logs the answer:

```
[info] server reports Spark 4.1.2
```

Then, **before** `CreateDataflowGraph`, it checks the pipeline's constructs
against that version. Today there is exactly one requirement: an **AUTO CDC**
flow needs a **Spark 4.2+** server. A too-old server is refused with a sentence,
and nothing is registered:

```
sdp: registration failed — Spark Connect server is too old for this pipeline:
AUTO CDC flow (SCD type 1) 'orders_cdc' (target 'dim_customers') needs a
Spark 4.2+ server; sc://localhost:15002 reports 4.1.2
```

Why a handshake at all, rather than letting the server complain? Because proto3
**silently drops unknown fields**. A 4.2-only message sent to a 4.1 server does
not arrive as "unsupported" — it arrives as a `DefineFlow` with no details, and
the server reports something confusing (or accepts a half-message). The
handshake turns that into one actionable line.

Deliberate asymmetries:

- **Version parsing is lenient**: only `major.minor` is read, from the front of
  the string, so `4.1.1`, `4.2.0-preview1` and vendor spellings like
  `4.1.0-amzn-0` all resolve.
- **An unreadable version never blocks.** If the server reports something this
  client cannot parse — or the probe itself fails — you get a warning and the run
  proceeds. Forks report odd strings, and a client that refuses to run against an
  unrecognised version is worse than one that tries.
- `sdpValidate` and `sdpManifest` are **offline** and never perform the
  handshake.

Escape hatch, should a server's version string ever be read wrongly:

```scala
sdpVersionCheck := false    // sbt; SDP_SKIP_VERSION_CHECK=true for SdpApp
```

Skipping logs a warning — it removes the only thing standing between a newer
construct and an older server.

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

Any Spark Connect server with the SDP surface (Spark 4.1+) is enough to
materialize real tables locally — e.g. an `apache/spark:4.1.1` container started
with `org.apache.spark.sql.connect.service.SparkConnectServer` and your table
format's jars on the classpath.

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
2. **Classload-eval** (D10): builds an isolated `URLClassLoader` over the
   project's runtime classpath — its parent is the JDK *platform* loader, so
   user and sdp classes resolve from your classpath and never delegate back to
   the plugin's own loader — then loads `sdpPipelineClass`, calls `pipeline`,
   and renders each fragment to a STRING via
   `dev.sdp.core.PipelineExport.encodeAll`. The fragment string is the *only*
   thing that crosses the classloader boundary, and the loader is `close()`d in
   a `finally`. (Nothing is expanded at compile time: evaluation happens here.)
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


## Environments: dev and prod targets

The dbt/DLT pattern translates directly: **code keeps unqualified dataset
names; the environment decides where they land.**

```scala
// build.sbt (local dev) — every managed table lands in dev_eric:
sdpDefaultCatalog  := "warehouse"
sdpDefaultDatabase := "dev_eric"
```

```bash
# prod (the SdpApp runner env — an Argo pod, a CI job):
SDP_DEFAULT_CATALOG=warehouse SDP_DEFAULT_DATABASE=analytics java -jar pipeline.jar run
```

Shared upstream *sources* (`externalTable("bronze.orders")`) stay qualified in
code, so every environment reads the same inputs — the dbt source/model split.

**The Nessie variant (recommended on the demo stack):** instead of renaming
schemas, pin a catalog to a branch (`spark.sql.catalog.warehouse_dev.ref =
dev-eric`) and point `sdpDefaultCatalog` at it — identical table names, an
isolated timeline, and an atomic `MERGE BRANCH dev-eric INTO main` promotes
every table the pipeline touched at once: the data pull-request.
