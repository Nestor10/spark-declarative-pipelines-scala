# Developing sbt-spark-pipelines

Contributor reference: every sbt command that matters in this repo, and the workflows built on
them. (For the plugin's *user-facing* tasks — `sdpManifest`, `sdpPush`,
`sdpImportSchemas` — see [`plugin.md`](plugin.md).)

## Modules

| sbt project | Path | Contents |
|---|---|---|
| `sdp` | `sdp/` | THE library: pure domain (`dev.sdp.core`), ZIO services (`dev.sdp.app`), runtime plan-builder DSL (`dev.sdp.dsl`), Spark Connect client + `SdpApp` (`dev.sdp.connect`) |
| `sbtSparkPipelines` | `sbt-spark-pipelines/` | The sbt plugin: classload-eval discovery, cached manifest task, run/watch/seed tasks over the library's Connect client |
| `root` | `.` | Aggregate only; never published |

## Everyday commands

```
sbt compile                      # all modules
sbt testFull                     # every zio-test suite, all modules
sbt sdp/testFull                 # the library (also: sbtSparkPipelines)
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

## Live-server integration tests (podman/docker required)

Gated — they skip unless enabled:

```
SDP_INTEGRATION=1 sbt 'sdp/Test/runMain dev.sdp.connect.AlgebraOracleSpec'
SDP_INTEGRATION=1 sbt 'sdp/Test/runMain dev.sdp.connect.PipelinesRegistrationIntegrationSpec'
SDP_INTEGRATION=1 sbt 'sdp/Test/runMain dev.sdp.connect.FunctionLibrarySpec'
```

Each suite starts (and always tears down) an `apache/spark:4.1.2` container via the
`podman`/`docker` CLI — no Testcontainers, engine-neutral. First run
pulls the image (multi-GB). The env var must reach the *forked test JVM*: if you started the
sbt server without it, `sbt shutdown` first, then run with the var set.

`AlgebraOracleSpec` is the **semantic oracle**: every algebra capability is verified through
the server's `AnalyzePlan` before being claimed in `SupportedCapabilities`.

## The coverage matrix — what we have and don't

```
sbt 'sbtSparkPipelines/Test/runMain dev.sdp.plugin.conformance.PrintCoverage'
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
   sbt 'sbtSparkPipelines/Test/runMain dev.sdp.plugin.conformance.RenderInventory \
     sbt-spark-pipelines/src/test/resources/spark-connect-inventory.txt'
   ```

3. **The git diff of the snapshot is the upstream change report.** Triage any new relation
   entries in `ConnectTiers` (the build fails until every one is classified).

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
  breaking changes bump the version and keep the parser multilingual.

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
