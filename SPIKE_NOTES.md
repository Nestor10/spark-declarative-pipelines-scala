# SPIKE: runtime plan-builder behind the DSL surface

Branch: `spike/runtime-builder`. All code additive, in package
`dev.sdp.dsl.runtime` inside `sdp-runtime-dsl`. No existing file modified.

## What was built

A *runtime* analogue of the compile-time `FlowExtractor` macro. Where the
macro walks the typed AST of a flow body and emits a `Rel`/`Ex` algebra tree at
`sbt compile`, the runtime builder produces the **identical** tree by ordinary
value-level method calls — every method on a builder value (`Df`, `Column`,
`GroupedDf`, `SourceB`, ...) appends one algebra node, eagerly, at call time.

Files added:

- `sdp-runtime-dsl/src/main/scala/dev/sdp/dsl/runtime/Runtime.scala` — the
  builder: `Column`/`Df`/`GroupedDf`/`WindowSpecB`, the `spark` reader facade
  (`spark.table`, `spark.read/readStream.format(...).schema(...).load()`,
  `spark.createDataFrame(Seq(...)).toDF(...)`), a `functions` facade
  (`col`/`lit`/`upper`/`row_number`/`sum`/`count`/`star`/`Window`), and the
  three entry points `externalTable` / `materializedView` / `streamingTable`
  plus `Pipeline(...)`.
- `.../runtime/RuntimeFixtures.scala` — the 6 Warehouse fragments, runtime-built.
- `.../runtime/MacroFixtures.scala` — the same 6 bodies, macro-built
  (`dev.sdp.dsl.*`), obtained exactly as `WarehouseDogfoodSpec` does.
- `.../runtime/EquivalenceSpec.scala` — DoD #1.
- `.../runtime/PipelineSpec.scala` — DoD #2.

## LOC

| | total lines | approx. non-comment/non-blank |
|---|---|---|
| `FlowExtractor.scala` (macro) | **1031** | ~720 |
| `Runtime.scala` (this spike, builder only) | **~283** | ~150 |

Caveat on the comparison: `FlowExtractor` covers the *entire* bounded flow
language (every `Rel`/`Ex` case — joins, set ops, na/stat namespaces, windows
with frames, lambdas, subqueries, DDL, inline data, ...). `Runtime.scala`
implements only the slice the Warehouse exercises. A full runtime port would
grow, but the per-operator cost is dramatically lower: each operator is one
`def` returning `Df(Rel.X(...))` (1–3 lines) versus a macro `case` that must
also pattern-match `Inlined`/`Typed`/`TypeApply` wrappers, thread an `Env` for
`val` bindings, and emit positioned `report.error`s on every failure branch.
Extrapolating the implemented slice, a complete runtime builder lands around
350–450 LOC — roughly **2.5–3x smaller** than the macro, and with no
`quotes.reflect` path-dependent-type tax.

## DoD #3 — diagnostics probe (macro `report.error` vs runtime)

This is the spike's most important finding, so it is recorded in detail. Three
classes of author mistake, what each frontend does:

### (a) Column typo inside a chain (`col("amout")` where schema is known)
- **Macro**: NOT a `report.error`. `col("amout")` is a valid string literal, so
  extraction succeeds; the bad column is caught later by `SchemaCheck` during
  `ManifestAssembly` →
  `PipelineValidationError.UnknownColumn("flow", "amout", available)`.
  (See `WarehouseDogfoodSpec` F13.) No source position; a build-task error.
- **Runtime**: IDENTICAL. `col("amout")` builds `Ex.Col("amout")`; the same
  `SchemaCheck` pass during `ManifestAssembly` produces the same
  `UnknownColumn` error. **No regression** — column-typo diagnostics live in the
  shared core, not in the frontend.

### (b) Dangling read (`spark.table("does_not_exist")`)
- **Macro**: builds fine (valid literal), fails at `ManifestAssembly` with
  `PipelineValidationError.DanglingEdges(List("does_not_exist"))`.
- **Runtime**: IDENTICAL — same `DanglingEdges` at assembly. **No regression.**

### (c) 2-node cycle
- **Macro**: `CycleDetected` at assembly.
- **Runtime**: IDENTICAL — `CycleDetected` at assembly (see `PipelineSpec`).
  **No regression.**

### THE ACTUAL DIFFERENCE — structural / non-literal mistakes
The macro's `report.error`-with-source-position fires on mistakes that are
*structural*, which the runtime builder cannot diagnose at all:

| mistake | macro frontend | runtime builder |
|---|---|---|
| `spark.table(someVar)` (non-literal name) | **compile error**, positioned: *"spark.table name must be a constant string literal; got: someVar"* | **compiles and runs silently** — any `String` value is accepted; the variable's runtime value flows straight into `Rel.NamedTable`. The "must be literal" guarantee is gone. |
| an unsupported method in the chain | **compile error**, positioned: *"Unsupported flow-body construct: ...; the flow language supports ..."* | the method **does not exist** on the builder type → an ordinary Scala *"value X is not a member of Df"* type error (still compile-time, still positioned, but generic Scala wording, not curated guidance). |
| `lit(someRuntimeInt)` (non-constant literal) | **compile error**: *"lit requires a constant literal"* | accepted — `lit(v: Int)` takes any `Int`. The constant-only guarantee is gone. |

Net: the runtime builder **keeps every graph-level diagnostic** (dangling,
cycle, unknown-column, inline-size) because those run in the shared core after
assembly. It **loses the macro's compile-time "must be a literal" guardrails**
and trades curated "unsupported construct" messages for the compiler's own
member-not-found errors. For mistakes that are genuinely *value-level* (typo'd
column names, wrong table names) there is no difference — and crucially those
are the mistakes authors actually make most.

## Surface fidelity — verdict

**The surface stayed faithful.** The six fixture bodies are *byte-for-byte
identical* between `RuntimeFixtures` and `MacroFixtures` except for the import
line (`dev.sdp.dsl.runtime.*` vs `dev.sdp.dsl.*`). Spark muscle memory carries
over unchanged: `spark.table`, `.groupBy(...).count()`, `.withColumn`,
`.select`, `readStream.format(...).schema(...).load()`,
`createDataFrame(Seq(...)).toDF(...)`, `row_number().over(Window.partitionBy(...).orderBy(...desc))`.

Compromises / gotchas encountered:

1. **`cols[S]` typed columns (SUBSTITUTED).** The reference `orders_enriched`
   body uses `cols[(order_id: Long, amount: Long, state: String)]` with
   `c.amount`. That relies on `Selectable.selectDynamic` whose field set is
   computed by a *type-level* `NamedTuple.Map` — a compile-time facility. A
   runtime builder cannot synthesize those phantom fields without a macro, so
   the fixture uses plain `col("amount")`. This is **render-invisible**: in the
   macro, `c.amount` lowers to `Ex.Col("amount")` too, so the produced `Rel` is
   identical. The *type-safety* of `cols[S]` (wrong name = type error) is the
   one thing genuinely lost; it would need its own macro/inline even in a
   runtime world. (Typed columns are secondary for this spike, as stated.)

2. **`createDataFrame` needs a typeclass.** The macro reads tuple *literals*
   from the AST. At runtime the builder needs the actual values, so
   `createDataFrame[A](Seq[A])` takes a `given InlineRows[A]` to turn each row
   into `List[LitValue]`. Only `(String, String)` is provided (all the
   Warehouse needs); a real port would derive `InlineRows` for arbitrary
   tuples/case classes (straightforward with a derivation or a handful of
   given instances). Minor, but it is extra machinery the macro didn't need.

3. **String vs Column overloads.** Mirrored Spark's `select`/`groupBy`
   String-and-Column overload pairs explicitly (as `FlowApi.scala` does) rather
   than via implicit conversions, to avoid `-feature` warning noise.

4. **`materializedView`/`streamingTable` overload split.** The macro module has
   SQL-string overloads of these names too; the runtime versions only take the
   `=> Df` body. No collision (different package), but a unified module would
   need to disambiguate by type exactly as `Dsl.scala` does today.

## Equivalence claim

`EquivalenceSpec` asserts `RelCodec.render(runtimeRel) == RelCodec.render(macroRel)`
for each fragment. `RelCodec.render` is the right oracle: it is the canonical,
byte-stable form the macro actually embeds (`render` → `parseTrusted`), so
comparing renders compares the algebra both frontends produce, modulo the
incidental `List`/`Set` ordering inside the surrounding `GraphFragment` (which
the round-trip reorders anyway). No normalization delta was needed — the trees
are expected to be structurally identical, not merely render-equal.

## Verification (run by the orchestrating session, 2026-06-11)

The implementing agent's sandbox denied `Bash`/Metals, so it delivered the code
hand-traced but uncompiled. The orchestrator then verified in this worktree:

- `sbt 'sdpRuntimeDsl/Test/compile'` — **compiles first try** (only
  pre-existing deprecation warnings from `DslSpec`).
- `sbt 'sdpRuntimeDsl/testOnly dev.sdp.dsl.runtime.*'` — **8/8 pass**: all
  EquivalenceSpec render-equality assertions hold (no normalization delta
  needed) and all three PipelineSpec scenarios (valid manifest, dangling read,
  cycle) behave as specified.
- `sbt 'sdpRuntimeDsl/testFull'` — **57/57 pass**; nothing existing regressed.
