# spark-declarative-pipelines-scala

**Type-safe Scala 3 authoring for Spark Declarative Pipelines, over Spark
Connect.**

Apache Spark 4.1's Declarative Pipelines (SDP) framework accepts pipelines only
as Python decorators or raw SQL. This project lets you declare the same
pipelines in ordinary Scala 3 — with the compiler, your IDE, and a build-time
validation pass on your side — and run them headless against any open Spark
Connect server:

- **Scala all the way down.** Datasets and flow bodies are plain Scala values
  (`spark.table(...).where(...).groupBy(...)`), so helpers, loops, conditionals
  and your own abstractions all work. Typed column references (`cols[S]`) make a
  misspelled column a *compile* error.
- **Invalid graphs never reach a cluster.** Cycles, duplicate datasets and
  dangling references fail in seconds, offline, before anything is sent.
- **No local Spark driver, no Python CLI.** The client speaks the SDP Protobuf
  surface directly over Spark Connect gRPC — plaintext for a local container,
  TLS + bearer token for a managed endpoint.
- **Library-first.** Your uber jar *is* the deployable: `java -jar app.jar run`.
  The sbt plugin is the fast inner loop over the very same code.

Status: published to Maven Central and validated end-to-end against a live
Spark 4.1.1 Connect server (batch, streaming, re-runs, periodic re-trigger).

## Quickstart

```scala
// project/plugins.sbt
addSbtPlugin("io.github.nestor10" % "sbt-spark-pipelines" % "0.1.0")
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

The plugin adds the matching `sdp` library itself (version lockstep), so one
line is all you write. Then declare the pipeline:

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

Inner loop (offline verdict in seconds, then the server's own analyzer):

```
sbt ~sdpValidate      # assemble + validate the graph; no server, no files
sbt sdpDryRun         # register server-side in validate-only mode
sbt sdpRun            # register and execute — materializes tables
```

Production: assemble the project and run the same object as a jar —
`java -jar app.jar run` — with configuration from the environment
(`SDP_CONNECT_ENDPOINT`, `SDP_STORAGE_ROOT`, `SDP_DEFAULT_DATABASE`, …). No sbt
on the path, which is what makes it schedulable from Argo/Kubernetes.

## Artifacts

Both are published to Maven Central under `io.github.nestor10` and move in
lockstep:

| Artifact | Coordinates | What it is |
|---|---|---|
| Library | `io.github.nestor10 %% "sdp" % "0.1.0"` | The DSL (`dev.sdp.dsl`), the pure algebra + validation (`dev.sdp.core`), the ZIO assembly services (`dev.sdp.app`), the Spark Connect client and `SdpApp` runner (`dev.sdp.connect`) |
| sbt plugin | `addSbtPlugin("io.github.nestor10" % "sbt-spark-pipelines" % "0.1.0")` | The inner loop: `sdpValidate`, `sdpManifest`, `sdpDryRun`, `sdpRun`, `sdpWatch`, `sdpSeed`, `sdpImportSchemas` |

Requirements: Scala 3, sbt 2, JDK 17+, and a Spark **4.1+** Connect server with
the Declarative Pipelines surface enabled.

## Documentation

- [Pipeline DSL](docs/dsl.md) — every combinator, typed columns, inline data,
  AUTO CDC, the manifest format, and the supported-Spark-surface statement.
- [The sbt plugin](docs/plugin.md) — tasks and settings, the inner loop,
  running for real, environments (dev/prod), TLS and tokens.
- [Developing](docs/developing.md) — contributor reference: test suites, the
  conformance/coverage matrix, local publishing, release discipline.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
