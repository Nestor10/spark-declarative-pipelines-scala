# spark-declarative-pipelines-scala

**Type-safe Scala 3 authoring for Spark Declarative Pipelines, over Spark
Connect.**

Apache Spark 4.1+ ships Declarative Pipelines (SDP), but accepts them only as
Python decorators or raw SQL. This project lets you declare the same pipelines
in ordinary Scala 3 — with the compiler, your IDE, and a build-time validation
pass on your side — and run them headless against any open Spark Connect
server:

- **Scala all the way down.** Datasets and flow bodies are plain Scala values
  (`spark.table(...).where(...).groupBy(...)`), so helpers, loops, conditionals
  and your own abstractions all work. Typed column references (`cols[S]`) make
  a misspelled column a *compile* error.
- **Invalid graphs never reach a cluster.** Cycles, duplicate datasets,
  dangling references and schema typos fail in seconds, offline, before
  anything is sent — then `sdpDryRun` lets the server's own Catalyst analyzer
  vet the graph without executing a thing.
- **Promotion is a hash, not a hope.** Environments are *connections*
  (`sdpTargets`), never code: the manifest your laptop validated is
  byte-identical to the one production runs — asserted by test, not promised
  by docs. Dev targets are user-scoped by default, so two engineers on one
  server never collide.
- **Version skew fails loudly.** The client resolves the server's Spark
  version before registering and refuses constructs the server can't honor
  (proto3 would otherwise drop them silently). AUTO CDC / SCD Type 1 runs
  live against Spark 4.2; SCD Type 2 is authored and validated today, and the
  wire gate flips itself when a release carries the bytes.
- **No local Spark driver, no Python CLI.** The client speaks the SDP Protobuf
  surface directly over Spark Connect gRPC — plaintext for a local container,
  TLS + bearer token for a managed endpoint.
- **Library-first.** Your uber jar *is* the deployable: `java -jar app.jar
  run`, configured entirely from the environment. The sbt plugin is the fast
  inner loop over the very same code. No sbt in production — which is what
  makes it schedulable from Argo/Kubernetes.

**Status.** Published to Maven Central; validated end-to-end against live
Spark 4.1 and 4.2 Connect servers — batch, streaming, re-runs, periodic
re-trigger, and AUTO CDC merges (on a merge-capable table format). Compliance
is measured, not asserted: the wire surface is drift-gated against Spark's own
protobuf descriptors, and a generated behavioral conformance report tracks
which *server behaviors* our test suites actually pin — gaps listed, on
purpose.

## Quickstart

```scala
// project/plugins.sbt
addSbtPlugin("io.github.nestor10" % "sbt-spark-pipelines" % "0.2.1")
```

```scala
// build.sbt
lazy val pipelines = (project in file("."))
  .enablePlugins(dev.sdp.plugin.SparkPipelinesPlugin)
  .settings(
    scalaVersion     := "3.9.0",
    sdpPipelineClass := "com.example.Warehouse",
  )
```

The plugin injects the matching `sdp` library itself (version lockstep), so
one line is all you write. Then declare the pipeline:

```scala
package com.example

import dev.sdp.dsl.*
import dev.sdp.connect.app.SdpApp

object Warehouse extends SdpApp:

  // a table that already exists in the catalog — read, not owned
  val orders = externalTable("bronze.orders")

  val cleaned = table("orders_clean") {
    spark.table("bronze.orders").where(col("amount") > lit(0L))
  }

  val daily = materializedView("daily_orders_by_state") {
    spark.table("orders_clean")
      .groupBy("state", "order_date")
      .count()
      .withColumnRenamed("count", "order_count")
  }

  def pipeline = Pipeline(orders, cleaned, daily)
```

The inner loop — offline verdict in seconds, then the server's own analyzer,
then a real run:

```
sbt ~sdpValidate      # assemble + validate the graph; no server, no files
sbt sdpDryRun         # register server-side in validate-only mode
sbt sdpRun            # register and execute — materializes tables
```

And the outer loop — name your environments once, then address them:

```scala
sdpTargets := Map(
  "dev"  -> SdpTarget.userScopedDev("sc://dev-connect:15002", catalog = "warehouse"),
  "prod" -> SdpTarget("sc://prod-connect:15002",
              defaultCatalog  = Some("warehouse"),
              defaultDatabase = Some("analytics"),
              useTls          = true,
              tokenEnv        = Some("PROD_CONNECT_TOKEN")),
)
```

```
sbt "sdpRunOn dev"        # user-scoped database, no collisions, no config
sbt "sdpDryRunOn prod"    # prod's real catalog validates it; nothing executes
```

Targets change *where* — endpoint, catalog, database, storage — never *what*:
the manifest bytes are identical across every target, so promoting to
production means running the hash you already tested. In production the same
object runs as a jar (`java -jar app.jar run`) with each target field supplied
as an `SDP_*` environment variable.

## Artifacts

Both are published to Maven Central under `io.github.nestor10` and move in
lockstep:

| Artifact | Coordinates | What it is |
|---|---|---|
| Library | `"io.github.nestor10" %% "sdp" % "0.2.1"` | The DSL (`dev.sdp.dsl`), the pure algebra + validation (`dev.sdp.core`), the ZIO assembly services (`dev.sdp.app`), the Spark Connect client + `SdpApp` runner (`dev.sdp.connect`) |
| sbt plugin | `addSbtPlugin("io.github.nestor10" % "sbt-spark-pipelines" % "0.2.1")` | The inner loop: `sdpValidate`, `sdpManifest`, `sdpDryRun`/`sdpRun` (+ `sdpRunOn`/`sdpDryRunOn`/`sdpSeedOn` per target), `sdpWatch`, `sdpSeed`, `sdpImportSchemas` |

Requirements: Scala 3, sbt 2, JDK 17+, and a Spark **4.1+** Connect server
with the Declarative Pipelines surface enabled (AUTO CDC needs **4.2+** — the
client checks and tells you).

## Documentation

- [Pipeline DSL](docs/dsl.md) — every combinator, typed columns, inline data,
  AUTO CDC / SCD, the manifest format, and the supported-Spark-surface
  statement.
- [Environments & promotion](docs/environments.md) — typed `sdpTargets`,
  user-scoped dev, `sdpDryRunOn prod` as the pre-merge gate, and
  byte-identical manifest promotion from a laptop to production.
- [The sbt plugin](docs/plugin.md) — tasks and settings, the inner loop,
  running for real, TLS and tokens, the server-version handshake.
- [Developing](docs/developing.md) — contributor reference: test suites
  (offline, live e2e, the behavioral conformance matrix), the upstream watch,
  local publishing, release discipline.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
