# Developing sbt-spark-pipelines

Contributor reference: every sbt command that matters in this repo, and the workflows built on
them. (For the plugin's *user-facing* tasks — `sdpManifest`, `sdpPush`,
`sdpImportSchemas` — see [`plugin.md`](plugin.md).)

## Modules

| sbt project | Path | Contents |
|---|---|---|
| `sdp` | `sdp/` | THE library: pure domain (`dev.sdp.core`), ZIO services (`dev.sdp.app`), runtime plan-builder DSL (`dev.sdp.dsl`), Spark Connect client + `SdpApp` (`dev.sdp.connect`) |
| `sbtSparkPipelines` | `sbt-spark-pipelines/` | The sbt plugin: classload-eval discovery, cached manifest task, run/watch/seed tasks over the library's Connect client |
| `sdpIt` | `sdp-it/` | Test-only, never published: the live-server integration/e2e suite (container-backed) + the behavioral conformance matrix. `dependsOn(sdp % "compile->compile;test->test")` |
| `root` | `.` | Aggregate only; never published |

`sdp`'s own suite is **structurally offline** — nothing on its test classpath can start a
container. Everything that needs a live Spark Connect server lives in `sdpIt`.

## Everyday commands

```
sbt compile                      # all modules
sbt testFull                     # every zio-test suite, all modules (sdpIt's gated specs skip)
sbt sdp/testFull                 # the library — offline, seconds (also: sbtSparkPipelines)
sbt sdpIt/testFull               # integration/e2e; gated specs need SDP_INTEGRATION=1 + podman
```

**Gotchas (sbt 2.0):**

- Plain `test` is *incremental* — it can run nothing and still say success. Use `testFull`.
- The thin client sometimes swallows test output. For reliable per-suite output, fork a runner:

  ```
  sbt 'sdp/Test/runMain dev.sdp.dsl.GoldenRenderSpec'
  ```

  (zio-test specs are mains; any FQN spec name works.)
- A wedged server: `sbt shutdown`, then retry.

## Scripted (plugin integration) tests

```
sbt sbtSparkPipelines/scripted                      # all sandboxes (~30s)
sbt 'sbtSparkPipelines/scripted sdp/valid-pipeline' # one sandbox
```

Sandboxes live under `sbt-spark-pipelines/src/sbt-test/sdp/*` (`valid-pipeline`,
`cyclic-pipeline`, `caching`). `scriptedDependencies` publishes all three modules to the local
ivy repo first, so scripted always tests the *current* code.

## Live-server integration tests — the `sdpIt` module (podman/docker required)

They live in `sdp-it/src/test` and are gated — they skip unless enabled:

```
SDP_INTEGRATION=1 sbt sdpIt/testFull                 # the whole suite
SDP_INTEGRATION=1 sbt 'sdpIt/Test/runMain dev.sdp.connect.AlgebraOracleSpec'
SDP_INTEGRATION=1 sbt 'sdpIt/Test/runMain dev.sdp.connect.PipelinesRegistrationIntegrationSpec'
SDP_INTEGRATION=1 sbt 'sdpIt/Test/runMain dev.sdp.connect.FunctionLibrarySpec'
SDP_INTEGRATION=1 sbt 'sdpIt/Test/runMain dev.sdp.connect.AutoCdcE2eSpec'    # 4.2.0 image
SDP_INTEGRATION=1 sbt 'sdpIt/Test/runMain dev.sdp.connect.IcebergAutoCdcE2eSpec'  # 4.2.0 + Iceberg
```

Each suite starts (and always tears down) a Spark container via the `podman`/`docker` CLI —
no Testcontainers, engine-neutral. The env var must reach the *forked test JVM*: if you
started the sbt server without it, `sbt shutdown` first, then run with the var set.

**Images.** The default is `apache/spark:4.1.2` (the conformance-oracle pin). `AutoCdcE2eSpec`
asks for `apache/spark:4.2.0` instead — AUTO CDC needs a 4.2+ server — which is a **separate
~1.3 GB pull**, several minutes the first time; `podman pull docker.io/apache/spark:4.2.0`
ahead of the run if you would rather watch the progress bar. `SDP_SPARK_IMAGE=<image>`
overrides every suite at once, which is how a candidate Spark build gets smoke-tested before
anything is pinned.

`AlgebraOracleSpec` is the **semantic oracle**: every algebra capability is verified through
the server's `AnalyzePlan` before being claimed in `SupportedCapabilities`.

`AutoCdcE2eSpec` (~50 s after the pull) registers a real AUTO CDC SCD1 pipeline against 4.2.0:
handshake, dry-run validation, the materialized target's schema, the server's identifier-only
rule for keys, and the `once` refusal. It deliberately asserts a **ceiling** rather than merge
results: AUTO CDC writes through DSv2 `MERGE`, and no *stock* Spark 4.2 table format
implements `SupportsRowLevelOperations` — parquet and Delta 4.4.0 are both refused with
`AUTOCDC_TARGET_DOES_NOT_SUPPORT_MERGE`. See behavioral row CDC-4.

### The Iceberg-flavored server — `IcebergAutoCdcE2eSpec` (rows CDC-5 / CDC-6)

The other side of that ceiling. Iceberg's `SparkTable` *does* implement
`SupportsRowLevelOperations`, so against an Iceberg-flavored server the same client, the same
flow and the same manifest get past the capability gate and the merge actually lands — which
is what makes the merge semantics (CDC-5) and the checkpointed re-run (CDC-6) assertable at
all. The spec seeds CDC events (including an out-of-order pair and a tombstone), runs, reads
the target back, appends more events, re-runs, and then reads the flow's **offset log** to
show that run 2 was micro-batch 1 of the same streaming query rather than a replay.

Two things make that server:

- **A pinned snapshot jar.** Iceberg has published no Spark-4.2 release; Apache's snapshot
  repository has one. `SparkConnectTestServer.Iceberg42` pins the **timestamped** artifact
  (`iceberg-spark-runtime-4.2_2.13-1.12.0-20260912.002841-8.jar`) rather than the bare
  `-SNAPSHOT` coordinate, because a snapshot coordinate serves different bytes tomorrow and
  this suite's verdict has to stay reproducible. It is downloaded **once** into
  `~/.cache/sdp-it/` (override with `SDP_IT_CACHE`) — outside the repo, so a 48 MB jar is
  never source — mounted read-only into the container and passed to
  `start-connect-server.sh` with `--jars`. It is **server fixture material only**: it must
  never appear in any `libraryDependencies`. First run needs network; later runs are offline.
  When Iceberg 1.12.0 ships to Central, swap that one line for the release —
  `scripts/upstream-watch.sh` watches for exactly that.
- **Catalog confs, not client changes.** `spark.sql.extensions`, `SparkSessionCatalog`, a
  hadoop catalog on a container-local warehouse, and `spark.sql.sources.default=iceberg`. The
  client sends **no format** anywhere (D13); the target is Iceberg because the catalog default
  says so, which is exactly how a user gets one.

`SparkConnectTestServer.Flavor` is the general shape (image + jars + confs), so the next suite
that needs a non-stock server does not need new plumbing.

## The coverage matrix — what we have and don't

```
sbt 'sdp/Test/runMain dev.sdp.connect.conformance.PrintCoverage'
```

Prints the full wire-surface matrix — per-tier relation coverage with the unsupported entries
listed, expression coverage, pipeline-command coverage, and untriaged counts:

```
Spark Connect Relation coverage (pinned artifact surface):
  T0Core          18 /  18  (100%)
  T1Extended      18 /  28  ( 64%)  as_of_join, catalog, html_string, lateral_join, ...
  T2Streaming      0 /   1  (  0%)  with_watermark
  T3Deferred       0 /   6  (  0%)  co_group_map, ...           (deferred by choice)
  NotApplicable    0 /   6  (  0%)  apply_in_pandas_with_state, ... (python-only/internal)
  untriaged relation entries:   none
  expressions supported:        6 / 22  (alias, cast, literal, ...)
  pipeline commands supported:  create_dataflow_graph, define_flow, define_output, start_run
```

The numbers are *generated* — capabilities come from the pinned artifact's protobuf
descriptors, claims from the oracle-verified `SupportedCapabilities` set. Nothing is
hand-maintained, so the matrix can't drift from reality. (The same report prints inside
`ConformanceSpec` during `testFull`.)

## Conformance harness

`ConformanceSpec` (runs in normal `testFull`) gates three things: the descriptor inventory
matches the golden snapshot (drift gate), every `Relation` oneof entry is tier-triaged, and
every capability claim names a real wire field. It also prints the coverage report
(T0/T1/expressions).

**On a Spark artifact upgrade** (bumping `spark-connect-common` in `build.sbt`):

1. The drift gate fails — expected.
2. Regenerate the snapshot:

   ```
   sbt 'sdp/Test/runMain dev.sdp.connect.conformance.RenderInventory \
     sdp/src/test/resources/spark-connect-inventory.txt'
   ```

3. **The git diff of the snapshot is the upstream change report.** Triage any new relation
   entries in `ConnectTiers` (the build fails until every one is classified).

## The behavioral matrix — the other half of conformance

```
sbt 'sdpIt/Test/runMain dev.sdp.connect.conformance.PrintBehaviorCoverage'
```

The wire matrix above proves *message-shape* coverage. It was at 100% on T0 when the
external-table gap shipped (D9), because reading a table you do not own is not a message —
it is a usage distinction in an existing one. Every expensive bug in this project's history
has lived in that blind spot: the graph-defaults edge drop, TRUNCATE-on-Delta, provider
re-assert on re-run, re-run NULL semantics.

So `BehaviorInventory` (`sdp-it` test sources — it lives beside the specs it cites, because
its `Covered` guard resolves them reflectively on one classpath) enumerates the *server
behaviors* an SDP client depends
on — registration sequence and error surfaces, graph defaults and name resolution, fresh run
vs re-run materialization, full refresh, `once` flows, AUTO CDC registration and execution,
external-input resolution, streaming triggers and run termination, event/progress wording,
dry-run semantics — one row each, with
a **Spark source anchor** (path → class.method in `../spark`) and an honest status:
`Covered(spec)` / `Uncovered` / `NotApplicable(reason)`.

Rules that keep it a measurement rather than a brochure, enforced by `BehaviorCoverageSpec`
in `sdpIt/testFull` (itself ungated — it needs no container, only the classpath):

- every row cites an anchor (the authority is the Spark source, not our memory);
- a `Covered` row must name a spec class that **resolves on the test classpath** — a renamed
  or deleted spec breaks the claim loudly;
- the whole `id → status` mapping is asserted against a literal snapshot, so coverage cannot
  drift in either direction without a deliberate edit;
- behavior verified **by hand** (a live session, a measured experiment) is still `Uncovered`,
  with the receipt in the row's note. Only an automated spec counts.

Most rows are Uncovered today, and that is the honest headline (15 / 49 in scope as of
2026-09-12): the container-gated `PipelinesRegistrationIntegrationSpec` covers the
registration handshake, dry-run validation, the dangling-upstream rejection and
external-input resolution failure; `AutoCdcE2eSpec` covers AUTO CDC registration, the
identifier-only rule, the materialized target schema, the MERGE-capability ceiling and the
`once` refusal; `IcebergAutoCdcE2eSpec` adds the SCD1 merge semantics and the checkpointed
re-run; re-run, full-refresh and graph-defaults behavior for ordinary tables is known only
from hand-run sessions. The report is generated, not committed — regenerate it whenever you want the
current accounting.

## Upstream watch — is the pin still current?

```
scripts/upstream-watch.sh                  # report to stdout, file nothing
scripts/upstream-watch.sh --no-inventory   # version + proto intel only (no build)
scripts/upstream-watch.sh --pin 4.1.2      # pretend we pin an older version
scripts/upstream-watch.sh --candidate 4.1.3  # "what would THIS release change?"
```

Runs weekly in CI (`.github/workflows/upstream-watch.yml`) and on demand. It reads the pin
out of `build.sbt`, asks Maven Central for newer `spark-connect-common_2.13` versions
(previews/RCs reported separately, and used as the candidate only when nothing stable is
newer), re-renders the conformance inventory **against the candidate artifact** and diffs it
against the committed snapshot, then reads `pipelines.proto` from apache/spark **master** for
the earliest possible intelligence about the next release. With drift, it opens-or-updates one
issue labelled `upstream-watch`; with none, it exits 0 silently. It never bumps the pin —
`protobuf-java`/`grpc` must match the release's own pom (D2), which is a human judgment.

It watches a **second** artifact for a different reason:
`org.apache.iceberg:iceberg-spark-runtime-4.2_2.13`. There is no release of it, which is why
`IcebergAutoCdcE2eSpec` pins an Apache *snapshot* jar — and a snapshot is mutable and
eventually swept, so the day a release appears is the day that pin must be swapped. Since
Iceberg is the only OSS format that makes AUTO CDC materialize at all, the report flags it as
**critical path** rather than housekeeping. Same mechanism as the Spark pin: one issue, never
an automatic bump. Today it reports `not yet released, snapshot in use` and exits 0.

Two mechanics worth knowing before touching it:

- The candidate render works because `build.sbt` reads the version from a **launch-time system
  property** (`sdp.connect.common.version`, default `4.2.0`). That is a load-time setting, not
  `sys.props` inside a cached task body — the thing the cache rules forbid.
- It must be passed as **`SBT_OPTS=-Dsdp.connect.common.version=…`**, and any warm sbt server
  must be stopped first: sbt 2's thin client does not forward a command-line `-D` to the server
  JVM, and a warm server has already evaluated `build.sbt`. Either mistake renders the
  inventory against the *current* pin and reports a clean diff — a false all-clear. The script
  therefore stops the server, passes `SBT_OPTS`, and **verifies from sbt's own
  `libraryDependencies` output** that the candidate was really resolved, failing loudly if not.
  (It also stops the server afterwards, so your next `sbt compile` is not silently on the
  candidate.)

## Publishing locally (for sandbox/e2e experiments)

```
sbt 'sdp/publishLocal' && sbt 'sbtSparkPipelines/publishLocal'
```

Then a scratch project (`project/plugins.sbt` → `addSbtPlugin("io.github.nestor10" % "sbt-spark-pipelines"
% "0.1.0-SNAPSHOT")`) can exercise the real task flow. **SNAPSHOT + warm caches caveat:** after
republishing the library or plugin, restart the scratch project's sbt server — a warm action
cache (and the metabuild's cached plugin closure) can otherwise serve stale results.

## Release discipline

- **Publish both artifacts from the same tag.** The fragment string is the contract between
  the library (encoder, on the user's classpath) and the plugin (parser, in the metabuild);
  the plugin injects its own version of `sdp` by default, so a mixed pair is a config mistake,
  not a supported combination.
- The manifest format (`sdp-manifest/2`, `/3` for AUTO CDC + `once`) is a frozen contract:
  breaking changes bump the version and keep the parser multilingual. SCD2 (roadmap S2) is
  the worked example of *not* bumping: its two track-history groups are optional and trailing
  in the AUTO CDC token stream and are emitted only when non-empty, so every manifest that
  predates them renders byte-identically and `/3` stayed `/3`. Prefer that shape — a new
  construct earns a version bump only when it changes bytes someone already wrote.

## Iterating against a separate consumer (e.g. `../sdp-example`)

A consumer that resolves the plugin from `publishLocal`/`publishM2` can pick up a **stale**
plugin after you republish a `-SNAPSHOT` — the consumer's metabuild caches the old plugin
closure (coursier + its `project/target`). It bites specifically on **codec/format changes**
(the manifest, `RelCodec`): the fragment *encoder* (the sdp library on the user classpath) and
the *parser* (the sdp library inside the plugin metabuild) must move in lockstep, and a warm
cache can bust only one side. Symptom: an undecodable fragment line,
or `NoClassDefFoundError`/`ZipException` at plugin load.

Fix (one command, from the main repo root), then restart the consumer's sbt session:

```
scripts/republish-and-reset-example.sh        # defaults to ../sdp-example
```

It republishes all three modules, evicts `dev.sdp` from the ivy + coursier caches, and clears
the consumer's metabuild/build targets. (All of this disappears once we cut immutable released
versions — `-SNAPSHOT` mutability is the root cause.)

## Where things are decided

Fragment discovery, the proto toolchain, the container oracle, the algebra, typed columns and
caching discipline are all settled by evidence (spikes, live-server receipts, minimal repros)
recorded in the maintainers' decision log and in the commit history. Re-open one of them with
evidence of the same grade, not a preference.
