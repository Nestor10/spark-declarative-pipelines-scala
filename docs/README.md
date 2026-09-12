# sbt-spark-pipelines — User Documentation

Start at the [project README](../README.md) for the pitch and a quickstart.

## Shipped

- **[Pipeline DSL](dsl.md)** — declare datasets in pure Scala 3 with the fluent
  flow language (`select`/`where`/`groupBy.agg`/joins/set-ops/…). Flow bodies are
  ordinary Scala that a **runtime plan-builder** turns into the typed relational
  algebra; the graph is validated at build time (cycles, lineage, gradual
  column-existence checking) before anything reaches a cluster. Also: `cols[S]`
  typed column references, inline data, SQL escape hatches, the canonical
  manifest format, and the supported-Spark-surface statement.
- **[The sbt plugin](plugin.md)** — `sdpValidate` (offline verdict in seconds),
  `sdpManifest` (cached; evaluates your pipeline object in an isolated
  classloader and fails the build on an invalid graph), `sdpDryRun` / `sdpRun` /
  `sdpWatch` (register, validate or really execute against a Spark Connect
  endpoint), `sdpSeed`, `sdpImportSchemas` (named-tuple schema codegen from your
  pipeline and the remote catalog), and every setting — including TLS and bearer
  tokens.
- **[Developing](developing.md)** — contributor command reference: tests
  (unit/scripted/live-integration), **the coverage matrix** (`PrintCoverage`),
  conformance/drift workflow on Spark upgrades, local publishing, release
  discipline.

## The shape of it

```
Scala 3 pipeline object (extends SdpApp)
  │  plan-builder evaluation → GraphFragment values
  ▼
assemble + validate (offline, deterministic: cycles, dangling reads, schemas)
  │  canonical manifest
  ▼
Spark Connect Protobuf over gRPC → any open Spark 4.1+ Connect server
```

The same `pipeline` value drives both surfaces: the sbt plugin evaluates it for
the dev loop, and `SdpApp` makes your uber jar the production runner
(`java -jar app.jar run`).
