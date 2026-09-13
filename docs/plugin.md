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
| `sdpPush` | task | **Deprecated — removed in 0.4.** Register the graph; dry or real per `sdpPushDryRun`. Use `sdpDryRun` / `sdpRun` (or the `*On` forms) instead |
| `sdpRun` | task | Register **and execute** the graph (dry = false, always) — materializes tables; one-shot run with progress |
| `sdpFullRefresh` | task | Register and execute the graph with `StartRun.full_refresh_all` — **the "rebuild everything" button**. The server rolls each streaming checkpoint to a new numbered sibling and recomputes every table from its sources. Destructive; see [Full refresh](#full-refresh--sdpfullrefresh) |
| `sdpFullRefreshOn <target>` | input task | `sdpFullRefresh` against a named environment (tab-completes the names) |
| `sdpWatch` | task | Re-trigger the pipeline every `sdpWatchInterval`s (Ctrl-C to stop) — client-side "continuous": each cycle **re-evaluates your pipeline** and is a triggered run whose AvailableNow resumes from the checkpoint and picks up new data (the server has no true continuous mode) |
| `sdpDumpWire` | task | Write the whole registration sequence to `<target>/sdp/wire/*.txtpb` as protobuf **text format** — the same commands `sdpRun`/`sdpDryRun` send. Offline and deterministic; see [Diagnostics](#diagnostics) |
| `sdpExplain [flow]` | input task | Ask the server to `EXPLAIN` each flow's relation (`AnalyzePlan`, extended) and print the plan plus one line per in-graph read. Registers nothing, runs nothing. Tab-completes flow names from the last manifest; see [Diagnostics](#diagnostics) |
| `sdpExplainOn <target> [flow]` | input task | `sdpExplain` against a named environment — the form that matters when two catalogs read the same bytes differently |
| `sdpSeed` | task | Run `sdpSeedStatements` (DDL/DML) against the server over Spark Connect — a local fixture to create + populate the source/catalog tables an `externalTable` reads, so a full run resolves them |
| `sdpTargets` | setting | Named environments as typed `SdpTarget` values (`Map[String, SdpTarget]`, default empty) — see [environments.md](environments.md) |
| `sdpRunOn <target>` | input task | `sdpRun` against a named environment from `sdpTargets` (tab-completes the names) |
| `sdpDryRunOn <target>` | input task | `sdpDryRun` against a named environment — full Catalyst validation against *that* catalog, zero execution: the pre-merge gate |
| `sdpSeedOn <target>` | input task | `sdpSeed` against a named environment |
| `sdpSeedStatements` | setting | SQL statements `sdpSeed` executes (e.g. `CREATE OR REPLACE TABLE bronze.orders USING delta AS SELECT …`). Default empty |
| `sdpConnectEndpoint` | setting | gRPC endpoint, `sc://host:port` (default `sc://localhost:15002`) |
| `sdpConnectUseTls` | setting | `true` to speak TLS (default `false` = plaintext — `sc://localhost` is the dev container) |
| `sdpConnectToken` | setting | Bearer token attached to every call (`Authorization: Bearer …`). Defaults to the `SDP_CONNECT_TOKEN` env var so the secret stays out of `build.sbt`; `""` = anonymous. Never logged |
| `sdpConnectDeadline` | setting | Per-RPC deadline in seconds for registration/seed/analyze calls (default 60) so a wedged server can't hang the build. The run stream is bounded by `sdpRunTimeout` instead |
| `sdpStorageRoot` | setting | Checkpoint/metadata root — absolute URI with scheme (default `file:///tmp/sdp/<project>`) |
| `sdpDefaultCatalog` | setting | Graph default catalog sent in `CreateDataflowGraph` (`""` = omit). Send it on named V2 catalogs — omission can silently drop dependency edges |
| `sdpDefaultDatabase` | setting | Graph default database — the dev/prod switch (see "Environments" below; `""` = omit) |
| `sdpPushDryRun` | setting | **Deprecated — removed in 0.4** with `sdpPush`. `true` (default): `sdpPush` validates only; `false`: it really runs. `sdpDryRun` / `sdpRun` do not read it |
| `sdpVersionCheck` | setting | `true` (default): ask the server which Spark it is before registering, and refuse constructs newer than it — see "Server-version handshake" below |
| `sdpImportSchemas` | task | Generate named-tuple schema aliases (for `cols[S]`) from the pipeline's inferred shapes + remote catalog tables |
| `sdpSchemasFile` | setting | Output for generated aliases (default `src/main/scala/sdp/schemas/PipelineSchemas.scala` — checked in, diffs reviewable) |
| `sdpSchemasPackage` | setting | Package of the generated aliases (default `sdp.schemas`) |
| `sdpCatalogTables` | setting | Remote tables to import (schema via the server's analyzer), e.g. `Seq("sales.orders")` |

> **AUTO CDC needs a Spark 4.2+ server.** `createAutoCdcFlow` pipelines encode
> on the wire and run like any other; `sdpValidate` / `sdpManifest` stay offline
> and work against any server or none. Point `sdpRun` / `sdpDryRun` at an older
> server and the handshake below refuses the pipeline before registering
> anything — `… needs a Spark 4.2+ server; sc://host:port reports 4.1.2`. See
> `docs/dsl.md`.

## Server-version handshake

Every task that talks to a server (`sdpDryRun`, `sdpRun`, `sdpWatch`,
and `SdpApp run` off sbt) asks it one question first — a single
`AnalyzePlan`/`SparkVersion` round trip on the same channel the registration
uses — and logs the answer:

```
[info] server reports Spark 4.1.2
```

Then, **before** `CreateDataflowGraph`, it checks the pipeline's constructs
against that version. Today there are two requirements, both AUTO CDC: an
**SCD type 1** flow needs a **Spark 4.2+** server, and an **SCD type 2** flow a
**Spark 4.3+** one (SCD2 is upstream-master only — see the SCD2 section of
`docs/dsl.md`, which also covers the separate, earlier refusal that applies until
the *client's* pinned proto carries those fields). A too-old server is refused
with a sentence, and nothing is registered:

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

## What `sdpDryRun` does

Reads the manifest from `sdpManifest`, then drives the registration
sequence over gRPC: `CreateDataflowGraph` → one `DefineOutput`/`DefineFlow`
per dataset → `StartRun`. The run is validate-only, so the server's Catalyst
analyzer fully resolves and validates the graph without executing anything:

```
[info] sdp: validating 3 dataset(s) on sc://localhost:15002 (build settings, dry=true, storage=file:///tmp/sdp/demo)
[info] sdp: pipeline validated (dry run) on the server; dataflow graph id: 8b744454-...
```

Failures are rendered by kind: an unreachable server reports as a transport
failure; an analyzer rejection (unresolvable dataset, invalid flow type)
reports the server's own diagnostic. If a later `DefineFlow` is the one that is
rejected, the half-built graph is dropped again server-side rather than left
behind.

> **`sdpPush` is deprecated** and will be removed in **0.4**. It did the same
> thing, dry or real depending on the separate `sdpPushDryRun` setting — a
> spelling that makes a build script ambiguous to read. Replace `sdpPush` with
> `sdpDryRun` or `sdpRun` (or `sdpDryRunOn <target>` / `sdpRunOn <target>`); it
> still works for now and warns, naming its replacement.

## Running for real — `sdpRun`

`sdpDryRun` validates only. To actually materialize tables, use `sdpRun`
(always `dry = false` — a dedicated task rather than a boolean setting). The
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

## Full refresh — `sdpFullRefresh`

`sdpRun` is incremental: a streaming flow resumes from its checkpoint, and only
data that arrived since the last run is processed. `sdpFullRefresh` is the other
button — Databricks calls it *Full Refresh* — and it means **throw the
incremental state away and rebuild from the sources**:

```
sbt:warehouse> sdpFullRefresh
[info] sdp: FULL-REFRESHING 4 dataset(s) on sc://localhost:15002 (build settings, dry=false, full-refresh=true, storage=file:///data/sdp-storage)
[info] sdp:   • Flow spark_catalog.default.silver is QUEUED.
...
[info] sdp: pipeline fully refreshed on the server; dataflow graph id: 0540ddba-...
```

`sdpFullRefreshOn <target>` is the same thing against a named environment (see
[environments.md](environments.md)).

### What the server actually does

Nothing is deleted from your machine: the client sets one field,
`StartRun.full_refresh_all` (proto field 3), and the *server* does the work. Its
semantics, read off the Spark 4.2.0 source (`State.reset`, `DatasetManager`):

- **Streaming checkpoints roll, they are not erased.** A flow's checkpoint lives
  at `<storageRoot>/_checkpoints/<catalog>/<db>/<table>/<flow>/<n>`, where `<n>`
  is an integer. A full refresh creates `<n+1>` and the query starts there from
  batch 0. **The old directory is left in place** — so a full refresh is
  recoverable-ish (the previous offsets are still on disk) and costs storage
  each time.
- **Reset happens BEFORE materialization.** Checkpoints roll first, then the
  targets are wiped, then flows run. There is no window in which a flow resumes
  from an old offset into a truncated table.
- **Materialized views are truncated on every run anyway.** An MV is rewritten
  by a literal `TRUNCATE TABLE` + insert whether or not you asked for a full
  refresh, so this task changes nothing for them. (It does mean the target's
  format must support `TRUNCATE` — Iceberg does, and a plain-parquet target does
  not.)
- **Streaming tables are reset and recomputed** — the point of the exercise.
- **AUTO CDC flows drop their auxiliary state table and replay the source.** The
  `__spark_autocdc_aux_state_<target>` table SDP maintains next to the target is
  part of the incremental state, so it goes with the checkpoint.

### The trap: `pipelines.reset.allowed=false`

> `pipelines.reset.allowed=false` behaves differently per mode: an explicit
> `full_refresh_selection` errors, while `full_refresh_all` **silently DEMOTES**
> the table to a plain refresh.

That is the exact rule from Spark's own `State.findFlowsToReset`, and it is the
first thing to check when a full refresh "didn't work": you get a green run, a
completed pipeline, and no reset — because the table asked not to be resettable
and `full_refresh_all` politely agreed. There is no warning on the wire and none
in the event stream. If a table must survive full refreshes, that property is how
you say so; if you are surprised by one that did, that property is why.

### When to reach for it

The canonical case is **a source whose history changed underneath you**: a CDC
feed reseeded, a bronze table backfilled with corrected rows, a schema
expectation that was wrong from the start. An incremental run cannot see any of
that — the checkpoint says those offsets are done. Before this task the fix was
to stop everything and `rm -rf` the checkpoint directory under the storage root
by hand (the sdp-example README keeps that dance only as a footnote for pins
older than 0.2.3); `sdpFullRefresh`
replaces it with one task that also handles truncation and AUTO CDC state, and
that works against a remote storage root you cannot `rm -rf` at all.

Reach for it when *correctness* requires a rebuild. Do not reach for it as a
retry: an ordinary `sdpRun` is the retry, and a full refresh throws away work
that was not wrong.

### From the uber jar

The same mode on the library-first runner, no sbt involved:

```bash
java -jar warehouse.jar run --full-refresh
```

`--dry --full-refresh` is **refused before anything is read or connected** — a
dry run validates and executes nothing, so there is no checkpoint to reset and no
table to rebuild. The plugin refuses the same combination in the same words; the
rule lives in one function (`SdpCommands.checkRunMode`) that both surfaces call.

## Watching — `sdpWatch`

`sdpWatch` re-triggers the pipeline every `sdpWatchInterval` seconds (default
30) until you press Ctrl-C. The server has no continuous execution mode, so a
"watch" is client-side periodic re-triggering: each cycle is an ordinary
`sdpRun`, and `Trigger.AvailableNow` means a streaming flow resumes from its
checkpoint and processes whatever arrived since.

Two properties worth knowing:

- **Every cycle re-evaluates your pipeline.** The classload-eval and validation
  re-run before each trigger, so an edit you make (and compile) while a watch is
  running is picked up by the *next* cycle — you are never watching stale code.
  Each cycle logs the shape it is about to run:

  ```
  [info] sdp: cycle — 4 dataset(s), 4 flow(s)
  ```

  A broken edit ends the watch with the same validation message `sdpValidate`
  would have printed, rather than quietly continuing on the old graph.
- **One wedged cycle cannot wedge the watch.** Cycles are bounded by
  `sdpRunTimeout` exactly as `sdpRun` is: an unbounded streaming source detaches
  (the server keeps going) and the watch moves on to the next cycle.

Each cycle registers a *fresh* dataflow graph, which is right for a dev loop but
does accumulate graph metadata server-side over a very long watch.

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
- **Byte-identical promotion.** The manifest does not depend on the
  environment. Registering against a different target — or repointing the whole
  build at a different endpoint, catalog, database and storage root — leaves the
  manifest's SHA-256 unchanged, so the artifact you validated in dev is the
  artifact prod runs. See [environments.md](environments.md).
- **Build-JVM hygiene.** The child classloader is closed per invocation and the
  ZIO runtime is task-scoped and torn down per invocation — safe for long-lived
  sbt servers. No ZIO runs inside the child loader; evaluation is plain code.

## Diagnostics

`sdpDumpWire` and `sdpExplain` are the two halves of a single question: *what
did we send, and how did the server read it?*

They exist because the SDP wire is a one-way mirror. The server logs nothing
about the plans it receives, and `pipelines.proto` has **no graph-readback
command** — once `CreateDataflowGraph` succeeds, the dataflow graph the server
built cannot be inspected by any client, ours or the official Python one. So
when a graph behaves unexpectedly, these two commands are the whole
observability story.

### `sdpDumpWire` — what we said

```
sbt sdpDumpWire
```

writes `<target>/sdp/wire/`:

```
00-create-dataflow-graph.txtpb
01-define-output-silver.txtpb
02-define-output-gold.txtpb
03-define-flow-silver.txtpb
04-define-flow-gold.txtpb
99-start-run.txtpb
```

Each file is one `spark.connect.PipelineCommand` in protobuf **text format**:

```protobuf
# sdp wire dump — 03-define-flow-silver.txtpb
# protobuf text format of one spark.connect.PipelineCommand, exactly as
# sdpRun/sdpDryRun sends it. The dataflow graph id is server-assigned;
# it is rendered here as "<dataflow-graph-id>".
define_flow {
  dataflow_graph_id: "<dataflow-graph-id>"
  flow_name: "silver"
  target_dataset_name: "silver"
  relation_flow_details {
    relation {
      ...
    }
  }
}
```

Properties worth relying on:

- **Same bytes.** The commands come from the encoder the live registration
  uses — there is no second rendering path that could drift.
- **Deterministic.** Equal manifests produce byte-identical files, so `diff`
  between two dumps means the pipeline changed. The one server-assigned value,
  the graph id, is a placeholder rather than a fresh UUID.
- **Round-trips.** `TextFormat.merge` parses any of these files back into the
  message it came from, so a dump can be replayed, edited or attached to a bug
  report.
- The `StartRun` shown is the **dry** one; a real `sdpRun` differs only in
  `dry`, which proto3 omits when false.

The same thing from the uber jar, no sbt needed: `java -jar app.jar dump-wire
[--out dir]` (default `./sdp-wire`).

### `sdpExplain` — how the server reads it

```
sbt sdpExplain                  # every flow
sbt "sdpExplain gold"           # one flow (tab-completes)
sbt "sdpExplainOn prod gold"    # against a named environment
```

`sdpExplain` sends `AnalyzePlan` with `EXPLAIN_MODE_EXTENDED` for each flow's
relation — the *same relation* the registration would send — and prints the
server's answer verbatim. Nothing is registered and nothing runs.

Two things to know about what you are looking at:

- It is a **standalone-session analysis**: the catalog resolves names, and there
  is no pipeline rewrite in front of it. That is deliberate — it shows how this
  server, with this catalog state, reads our bytes.
- Consequently, the answer depends on catalog state. Explaining the same
  pipeline before and after its tables exist gives different output; that
  difference is often the interesting part.

On a clean catalog, flows that read not-yet-existing tables come back with the
server's `TABLE_OR_VIEW_NOT_FOUND` message instead of a plan. That is printed as
information, not as an error: the task still succeeds.

#### Reading the Parsed Logical Plan

`EXTENDED` output has four sections. The first one, `== Parsed Logical Plan ==`,
is the one these diagnostics care about: it is `SparkConnectPlanner`'s raw decode
of our bytes, *before* the analyzer resolves anything. The later sections
(Analyzed / Optimized / Physical) are all fully resolved by definition and say
nothing about how the plan arrived.

A flow that reads another dataset in the same graph shows up there in one of two
shapes:

```
== Parsed Logical Plan ==
'Project ['id]
+- 'UnresolvedRelation [silver_pe], [], false             <- arrives as a name
```

```
== Parsed Logical Plan ==
Project [id#23, amount#24, 1 AS tag#28]
+- SubqueryAlias spark_catalog.default.silver_pe          <- arrives resolved
   +- Relation spark_catalog.default.silver_pe[id#23,amount#24] parquet
```

After the plan, `sdpExplain` prints one line per in-graph read saying which
shape it found:

```
--- in-graph reads, as the Parsed Logical Plan above shows them (HEURISTIC) ---
read 'silver_pe': UnresolvedRelation in Parsed plan
```

```
--- in-graph reads, as the Parsed Logical Plan above shows them (HEURISTIC) ---
read 'silver_pe': already resolved in Parsed plan (Relation)
note: a read that arrives pre-resolved is not registered as a pipeline
dependency by Spark 4.2.x — see the upstream issue below
```

The classification is a **heuristic over Spark's rendered plan text** (plan
rendering is not an API), which is why it is labelled as one, why it declines to
guess when it cannot find a read, and why the plan itself is always printed
above it. External tables are not classified — they live in the catalog by
definition.

#### The upstream behavior, stated plainly

`SparkConnectPlanner.transformWithColumns`, and a number of sibling transforms,
analyze their child eagerly while *decoding* the request. Once an in-graph
upstream table exists in the catalog, a read underneath one of them therefore
arrives at the pipeline already resolved, carrying no name for the graph to
recognise — and Spark 4.2.x registers no dependency edge for it. Nothing fails;
the flows are simply not ordered.

This is an upstream issue, filed against Apache Spark: **SPARK-XXXXX**
*(placeholder — replace with the issue id once it is assigned)*. This project
does not work around it: `sdpExplain` reports what the server did so the
situation is visible, and the fix belongs upstream.

Measured, not inferred: `PlanExplainE2eSpec` runs this experiment against a live
`apache/spark:4.2.0` — identical bytes, one `CREATE TABLE` in between, and the
classification flips from `UnresolvedRelation` to `already resolved`.

## The manifest artifact

`<target>/sdp/pipeline.sdpm`, format `sdp-manifest/2` — canonical and
byte-stable (sorted entries, percent-encoded fields, no timestamps). See
[the DSL doc](dsl.md#the-manifest) for the format itself.


## Environments: dev and prod targets

**→ The full story, including the promotion chain, is
[docs/environments.md](environments.md).**

The dbt/DLT pattern translates directly: **code keeps unqualified dataset
names; the environment decides where they land.** Two ways to say it.

**Flat settings** — one implicit environment, wherever this build points:

```scala
// build.sbt (local dev) — every managed table lands in dev_eric:
sdpDefaultCatalog  := "warehouse"
sdpDefaultDatabase := "dev_eric"
```

**Named targets** (`sdpTargets`) — several environments, addressed by name:

```scala
sdpTargets := Map(
  "dev"  -> SdpTarget.userScopedDev("sc://localhost:15002", catalog = "warehouse"),
  "prod" -> SdpTarget(
    connectEndpoint = "sc://spark-connect.prod.svc:15002",
    defaultCatalog  = Some("warehouse"),
    defaultDatabase = Some("analytics"),
    storageRoot     = Some("s3a://lake/sdp/prod"),
    useTls          = true,
    tokenEnv        = Some("SDP_PROD_TOKEN"),   // the NAME of an env var, never a secret
  ),
)
```

```
sbt:warehouse> sdpRunOn dev        # your own schema: dev_<user>, collision-free
sbt:warehouse> sdpDryRunOn prod    # prod's real catalog, zero execution — the pre-merge gate
```

```bash
# prod (the SdpApp runner env — an Argo pod, a CI job): each SdpTarget field is
# the SDP_* variable the runner already reads.
SDP_DEFAULT_CATALOG=warehouse SDP_DEFAULT_DATABASE=analytics java -jar pipeline.jar run
```

A target varies **where** a pipeline runs, never **what** it is: `sdpManifest`
and `sdpValidate` never read `sdpTargets`, so every environment registers the
*same manifest bytes* — the hash you validated in dev is the hash prod runs
(asserted by the `sdp/targets` scripted suite).

Shared upstream *sources* (`externalTable("bronze.orders")`) stay qualified in
code, so every environment reads the same inputs — the dbt source/model split.

**The Nessie variant (recommended on the demo stack):** instead of renaming
schemas, pin a catalog to a branch (`spark.sql.catalog.warehouse_dev.ref =
dev-eric`) and point the target's `defaultCatalog` at it — identical table
names, an isolated timeline, and an atomic `MERGE BRANCH dev-eric INTO main`
promotes every table the pipeline touched at once: the data pull-request.
