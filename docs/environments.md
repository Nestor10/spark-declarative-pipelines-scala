# Environments & promotion

> Local blast → `sdpRunOn dev` in your own schema → `sdpDryRunOn prod` against
> the real production catalog → merge → the **same manifest bytes** run in prod.

This is the "targets" ergonomic (dbt profiles, Databricks Asset Bundles) with
one property those tools cannot offer: **the artifact you validated is the
artifact that runs**. Two principles make that true, and everything below is
downstream of them.

**P1 — parameters vary WHERE, never WHAT.** A target changes the endpoint, the
catalog/database unqualified datasets land in, the storage root, and how to
authenticate. It cannot change the graph. There is no environment-conditional
dataset construction, on purpose: `if (env == "prod")` in a pipeline destroys
the promotion guarantee, and you already have a host language for the cases that
genuinely differ — the *output* must be one graph.

**P2 — YAML is output, not source of truth.** The descriptor is typed Scala in
`build.sbt`. YAML appears only as something we *emit* and you review (the future
`sdpExportArgo`) or as 12factor environment variables on the runner. There is no
hand-edited `sdp.yaml` competing with the build.

**The headline property: byte-identical manifest promotion.** The manifest is
canonical and byte-stable, and targeting rides on wire-level registration
defaults (`CreateDataflowGraph`'s `default_catalog`/`default_database`), so dev
and prod register *the same bytes*. The hash you tested in dev is the hash Argo
runs in prod. This is asserted, not asserted-to: the scripted suite
`src/sbt-test/sdp/targets` moves every environment knob in the build — endpoint,
catalog, database, storage root, target map — and compares manifest SHA-256s
across the swap.

## Declaring environments

```scala
// build.sbt
sdpTargets := Map(
  // Collision-free by default: the database is dev_<your user name>.
  "dev"  -> SdpTarget.userScopedDev("sc://localhost:15002", catalog = "warehouse"),

  "prod" -> SdpTarget(
    connectEndpoint = "sc://spark-connect.prod.svc.cluster.local:15002",
    defaultCatalog  = Some("warehouse"),
    defaultDatabase = Some("analytics"),
    storageRoot     = Some("s3a://lake/sdp/prod"),
    useTls          = true,
    tokenEnv        = Some("SDP_PROD_TOKEN"),   // the NAME of an env var
  ),
)
```

| Field | Meaning | Omitted (`None`) means |
|---|---|---|
| `connectEndpoint` | `sc://host:port` — the only required field | — |
| `defaultCatalog` | graph default catalog (`CreateDataflowGraph` field 1) | fall back to `sdpDefaultCatalog` |
| `defaultDatabase` | graph default database (field 2) — where unqualified managed datasets land | fall back to `sdpDefaultDatabase` |
| `storageRoot` | checkpoint/metadata root, absolute URI with a scheme | fall back to `sdpStorageRoot` |
| `useTls` | TLS to this endpoint (`Boolean`, default `false`) | n/a — a target always decides, because TLS is a property of the endpoint |
| `tokenEnv` | **name** of the environment variable holding the bearer token | fall back to `sdpConnectToken` (itself defaulting to `SDP_CONNECT_TOKEN`) |
| `deadlineSeconds` | per-RPC deadline for this environment | fall back to `sdpConnectDeadline` |
| `versionCheck` | server-version handshake for this environment | fall back to `sdpVersionCheck` |
| `schedule`, `namespace`, `runnerImage` | **Argo-reserved, inert today** — consumed by the future `sdpExportArgo` | — |

**Secrets are unrepresentable.** There is no `token: String` field. Only
`tokenEnv`, the *name* of a variable, resolved when the task runs — so a literal
credential cannot end up in a committed `build.sbt`, in `show sdpTargets`, or in
a CI log. A missing variable fails before any socket is opened:

```
sdp: target 'prod' authenticates with the environment variable 'SDP_PROD_TOKEN',
but 'SDP_PROD_TOKEN' is unset or empty. Export it in the shell that runs sbt
(the value is read at task time; it is never stored in build.sbt and never logged).
```

Targets are validated **when used**, not at build load: a typo in a prod target
must never break someone's offline `sdpValidate`. Unknown names answer with the
list:

```
sdp: unknown target 'produciton'. Available targets: dev, prod.
```

## The chain

### 1. Local blast — a container you can destroy

Start any Spark 4.1+ Connect server with the SDP surface (the demo stack in
`sdp-example` is Spark 4.1.1 + Iceberg + Nessie) and use the ordinary
untargeted tasks against the flat settings: `sdpSeed`, then `~sdpValidate` for
the offline loop, then `sdpRun`. Nothing is shared, so nothing can be broken for
anyone else. This is where you iterate.

### 2. `sdpRunOn dev` — a real catalog, your own schema

```
sbt:warehouse> sdpRunOn dev
[info] sdp: running 5 dataset(s) on sc://localhost:15002 (target 'dev', dry=false, storage=file:///tmp/sdp/warehouse)
```

`SdpTarget.userScopedDev` sets `defaultDatabase` to `dev_<sanitized user.name>`
(`Eric Smith` → `dev_eric_smith`), so two engineers pointed at the same server
never write the same tables and neither configures anything. Shared upstream
*sources* stay qualified in code (`spark.read.table("bronze.orders")`), exactly
the dbt source/model split: every environment reads the same inputs and writes
its own outputs.

**The Nessie variant (recommended on the demo stack).** Instead of renaming
schemas, pin a *catalog* to a branch and point the target's `defaultCatalog` at
it:

```
spark.sql.catalog.warehouse_dev.ref = dev-eric
```

```scala
"dev" -> SdpTarget("sc://localhost:15002", defaultCatalog = Some("warehouse_dev"))
```

Identical table names, an isolated timeline, and one atomic
`MERGE BRANCH dev-eric INTO main` promotes every table the pipeline touched —
the data pull-request. Verified on the demo stack (see the Iceberg/Nessie notes
in the repo README).

### 3. `sdpDryRunOn prod` — the pre-merge gate

```
sbt:warehouse> sdpDryRunOn prod
[info] sdp: validating 5 dataset(s) on sc://spark-connect.prod.svc.cluster.local:15002 (target 'prod', dry=true, storage=s3a://lake/sdp/prod)
[info] server reports Spark 4.2.0
[info] sdp: pipeline validated (dry run) on the server; dataflow graph id: 8b744454-…
```

This is the step DAB does not have. The **real production catalog** resolves
every read, the real Catalyst analyzer type-checks every expression, and **zero
flows execute** — nothing is written, no table is created. A column that exists
in your dev fixture but not in prod fails here, in seconds, before the merge.

Run it in CI on the pull request: it needs only network access to the prod
Connect endpoint and a read-only-ish credential, and it is the strongest
pre-merge signal available short of running the pipeline.

### 4. Merge → prod runs the same bytes

Production does not use sbt. The `SdpApp` uber jar is the runner, and **each
`SdpTarget` field maps 1:1 onto the `SDP_*` variable it already reads** — that
mapping *is* production target selection (12factor):

| `SdpTarget` field | Runner environment variable |
|---|---|
| `connectEndpoint` | `SDP_CONNECT_ENDPOINT` |
| `defaultCatalog` | `SDP_DEFAULT_CATALOG` |
| `defaultDatabase` | `SDP_DEFAULT_DATABASE` |
| `storageRoot` | `SDP_STORAGE_ROOT` |
| `useTls` | `SDP_CONNECT_USE_TLS` |
| `tokenEnv` | the variable it names → `SDP_CONNECT_TOKEN` |
| `deadlineSeconds` | `SDP_CONNECT_DEADLINE` |
| `versionCheck` | `SDP_SKIP_VERSION_CHECK` (inverted) |
| — | `SDP_RUN_TIMEOUT`, `SDP_PIPELINE_NAME` |

```bash
SDP_CONNECT_ENDPOINT=sc://spark-connect.prod.svc:15002 \
SDP_DEFAULT_CATALOG=warehouse \
SDP_DEFAULT_DATABASE=analytics \
SDP_STORAGE_ROOT=s3a://lake/sdp/prod \
SDP_CONNECT_USE_TLS=true SDP_CONNECT_TOKEN=$SDP_PROD_TOKEN \
java -jar warehouse-assembly.jar run
```

Nothing about the jar differs per environment, and the manifest it evaluates is
the same graph the `sdpDryRunOn prod` gate approved. When `sdpExportArgo` lands
it consumes `SdpTarget` unchanged — `schedule`/`namespace`/`runnerImage` are
already on the model — and emits a `CronWorkflow` + `ConfigMap` you review in a
PR and `kubectl apply`. YAML stays output (P2).

## The server-version handshake, per target

Every task that talks to a server asks it which Spark it is first, and refuses a
pipeline whose constructs are newer (AUTO CDC SCD1 needs 4.2+, SCD2 needs 4.3+).
That runs **per target**, which is the point: your dev container and your prod
cluster are usually not the same version, and the answer you need is about the
environment you are about to register into. Per-target `versionCheck = Some(false)`
exists for a fork whose version string this client reads wrongly; see
[the handshake section of plugin.md](plugin.md#server-version-handshake).

## What is deliberately NOT here

- **No env-conditional graphs.** See P1. If two environments need different
  datasets, they are different pipelines.
- **No secrets in the build.** See `tokenEnv`.
- **No target in `sdpManifest` / `sdpValidate`.** Those tasks do not read
  `sdpTargets`, `sdpConnectEndpoint`, or anything else connection-shaped — and
  cannot: sbt 2 requires a cache-hash instance for every setting a cached task
  reads, and `SdpTarget` deliberately has none, so a cached task that reached
  for an environment would not compile. The offline loop stays offline and the
  manifest stays a pure function of your sources.
- **No `sdpImportSchemasOn`.** Schema codegen is an authoring aid, not a
  deployment; it reads the flat settings.

## Reference

| Task | What it does |
|---|---|
| `sdpRunOn <target>` | `sdpRun` against that environment (materializes tables) |
| `sdpDryRunOn <target>` | `sdpDryRun` against that environment (validate only) |
| `sdpSeedOn <target>` | `sdpSeedStatements` against that environment |
| `sdpTargets` | `Map[String, SdpTarget]`, default empty |

`SdpTarget.userScopedDev(endpoint, catalog)` builds the user-scoped dev target.
Tab-completion over the declared names works on all three tasks.
