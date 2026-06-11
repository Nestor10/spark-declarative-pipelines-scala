# sbt-spark-pipelines — User Documentation

Documentation grows here as features leave [`ROADMAP.md`](../ROADMAP.md).

## Shipped

- **[Pipeline DSL](dsl.md)** — declare datasets in pure Scala 3 with the
  fluent flow language (`select`/`where`/`groupBy.agg`/joins/set-ops/...),
  compile-time extraction and validation (cycles, lineage, gradual
  column-existence checking), `cols[S]` typed column references, SQL escape
  hatches, the canonical manifest format, and the supported-Spark-surface
  statement.
- **[The sbt plugin](plugin.md)** — `sdpManifest` (cached, TASTy-based
  discovery, build-failing validation), `sdpPush` (register + dry-run
  or run against a Spark Connect endpoint), `sdpImportSchemas` (named-tuple
  schema codegen from your pipeline and the remote catalog), and all settings.
- **[Developing](developing.md)** — contributor command reference: tests
  (unit/scripted/live-integration), **the coverage matrix** (`PrintCoverage`),
  conformance/drift workflow on Spark upgrades, local publishing, release
  discipline.

## Pointers

- What's being built next → [`ROADMAP.md`](../ROADMAP.md)
- Why it's designed this way → [`DECISIONS.md`](../DECISIONS.md)
- Architecture and contributor guidance → [`CLAUDE.md`](../CLAUDE.md) and
  [`context/`](../context/)
