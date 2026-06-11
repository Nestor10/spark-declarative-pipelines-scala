package dev.sdp.dsl.runtime

import dev.sdp.core.GraphFragment
import dev.sdp.core.algebra.{Rel, RelCodec}
import zio.test.*

/** Corpus-driven parity spec (D10 / M1): for EVERY construct in the macro
  * frontend's coverage corpus (`FlowLanguageSpec` + `WarehouseDogfoodSpec`),
  * the runtime builder's `Rel` renders IDENTICALLY (via `RelCodec.render`) to
  * the macro frontend's `Rel` for the same body — body-for-body, only the
  * import differing.
  *
  * `RelCodec.render` is the equivalence oracle: a pure function of tree
  * structure and the exact form the macro embeds (render → parseTrusted), so
  * comparing renders compares the algebra both frontends produce, ignoring
  * incidental List/Set ordering in the surrounding `GraphFragment`.
  *
  * Each corpus entry is its own named test so a failure names the construct
  * (e.g. "functionsFacade: runtime Rel renders identically to macro Rel").
  */
object EquivalenceSpec extends ZIOSpecDefault:

  private def relOf(frag: GraphFragment): Option[Rel] =
    frag.flows.headOption.map(_.relation)

  private def sameRel(name: String, runtime: GraphFragment, macroF: GraphFragment) =
    test(s"$name: runtime Rel renders identically to macro Rel") {
      (relOf(runtime), relOf(macroF)) match
        case (Some(r), Some(m)) =>
          assertTrue(RelCodec.render(r) == RelCodec.render(m))
        case _ =>
          assertTrue(false) // both fragments must carry a flow
    }

  // ------------------------------------------------------------------
  // node-shape equivalence for external tables (no flow / no Rel)
  // ------------------------------------------------------------------
  private val extTableTests = test("externalTable: node shape matches macro frontend") {
    assertTrue(
      RuntimeFixtures.bronzeOrders.nodes == MacroFixtures.bronzeOrders.nodes,
      RuntimeFixtures.bronzeCustomerOrders.nodes == MacroFixtures.bronzeCustomerOrders.nodes,
      RuntimeFixtures.bronzeOrders.flows.isEmpty,
      MacroFixtures.bronzeOrders.flows.isEmpty,
    )
  }

  // SQL-string-backed entry points: the flow's Rel is Rel.Sql (sqlStreamingTable)
  // or there is no flow (materializedView(name)(sql) / temporaryView).
  private val sqlEntryPointTests = test("SQL entry points match macro frontend") {
    val rtSqlST = sqlStreamingTable("sql_st")("SELECT 1")
    val rtMv    = materializedView("mv_sql")("SELECT 2")
    val rtTv    = temporaryView("tv_sql")("SELECT 3")
    assertTrue(
      rtSqlST.flows.map(_.relation) == List(Rel.Sql("SELECT 1")),
      rtMv.flows.isEmpty,
      rtMv.nodes.map(_.id) == List("mv_sql"),
      rtTv.nodes.map(_.id) == List("tv_sql"),
    )
  }

  /** The corpus: one (name, runtime, macro) triple per construct. */
  private val corpus: List[(String, GraphFragment, GraphFragment)] = List(
    ("sourceOptionWhereSelect", CorpusRuntimeFixtures.sourceOptionWhereSelect, CorpusMacroFixtures.sourceOptionWhereSelect),
    ("valIntermediates",        CorpusRuntimeFixtures.valIntermediates,        CorpusMacroFixtures.valIntermediates),
    ("inlineToDf",              CorpusRuntimeFixtures.inlineToDf,              CorpusMacroFixtures.inlineToDf),
    ("inlineMixed",             CorpusRuntimeFixtures.inlineMixed,             CorpusMacroFixtures.inlineMixed),
    ("inlineSingleColumn",      CorpusRuntimeFixtures.inlineSingleColumn,      CorpusMacroFixtures.inlineSingleColumn),
    ("groupAggJoinOrderLimit",  CorpusRuntimeFixtures.groupAggJoinOrderLimit,  CorpusMacroFixtures.groupAggJoinOrderLimit),
    ("castSampleHint",          CorpusRuntimeFixtures.castSampleHint,          CorpusMacroFixtures.castSampleHint),
    ("repartitionNaStat",       CorpusRuntimeFixtures.repartitionNaStat,       CorpusMacroFixtures.repartitionNaStat),
    ("unpivotObserve",          CorpusRuntimeFixtures.unpivotObserve,          CorpusMacroFixtures.unpivotObserve),
    ("exprStarWindow",          CorpusRuntimeFixtures.exprStarWindow,          CorpusMacroFixtures.exprStarWindow),
    ("containerAccess",         CorpusRuntimeFixtures.containerAccess,         CorpusMacroFixtures.containerAccess),
    ("lambdasNamed",            CorpusRuntimeFixtures.lambdasNamed,            CorpusMacroFixtures.lambdasNamed),
    ("subqueries",              CorpusRuntimeFixtures.subqueries,              CorpusMacroFixtures.subqueries),
    ("tastyRoundTrip",          CorpusRuntimeFixtures.tastyRoundTrip,          CorpusMacroFixtures.tastyRoundTrip),
    ("facadeTable",             CorpusRuntimeFixtures.facadeTable,             CorpusMacroFixtures.facadeTable),
    ("facadeSql",               CorpusRuntimeFixtures.facadeSql,               CorpusMacroFixtures.facadeSql),
    ("facadeRange1",            CorpusRuntimeFixtures.facadeRange1,            CorpusMacroFixtures.facadeRange1),
    ("facadeRange3",            CorpusRuntimeFixtures.facadeRange3,            CorpusMacroFixtures.facadeRange3),
    ("facadeReadTable",         CorpusRuntimeFixtures.facadeReadTable,         CorpusMacroFixtures.facadeReadTable),
    ("facadeReadStream",        CorpusRuntimeFixtures.facadeReadStream,        CorpusMacroFixtures.facadeReadStream),
    ("facadeStreamSource",      CorpusRuntimeFixtures.facadeStreamSource,      CorpusMacroFixtures.facadeStreamSource),
    ("facadeBatchSource",       CorpusRuntimeFixtures.facadeBatchSource,       CorpusMacroFixtures.facadeBatchSource),
    ("guideExample",            CorpusRuntimeFixtures.guideExample,            CorpusMacroFixtures.guideExample),
    ("sugarStringsSortFilter",  CorpusRuntimeFixtures.sugarStringsSortFilter,  CorpusMacroFixtures.sugarStringsSortFilter),
    ("sugarIsin",               CorpusRuntimeFixtures.sugarIsin,               CorpusMacroFixtures.sugarIsin),
    ("sugarNaFill",             CorpusRuntimeFixtures.sugarNaFill,             CorpusMacroFixtures.sugarNaFill),
    ("sugarStatCorr",           CorpusRuntimeFixtures.sugarStatCorr,           CorpusMacroFixtures.sugarStatCorr),
    ("withColumnsMaps",         CorpusRuntimeFixtures.withColumnsMaps,         CorpusMacroFixtures.withColumnsMaps),
    ("functionsFacade",         CorpusRuntimeFixtures.functionsFacade,         CorpusMacroFixtures.functionsFacade),
    ("hofsNoWrapper",           CorpusRuntimeFixtures.hofsNoWrapper,           CorpusMacroFixtures.hofsNoWrapper),
    ("ddlSchema",               CorpusRuntimeFixtures.ddlSchema,               CorpusMacroFixtures.ddlSchema),
    ("withSchemaSource",        CorpusRuntimeFixtures.withSchemaSource,        CorpusMacroFixtures.withSchemaSource),
    ("crossModulo",             CorpusRuntimeFixtures.crossModulo,             CorpusMacroFixtures.crossModulo),
    ("booleanAnd",              CorpusRuntimeFixtures.booleanAnd,              CorpusMacroFixtures.booleanAnd),
    ("dropDuplicatesOne",       CorpusRuntimeFixtures.dropDuplicatesOne,       CorpusMacroFixtures.dropDuplicatesOne),
    ("joinDrop",                CorpusRuntimeFixtures.joinDrop,                CorpusMacroFixtures.joinDrop),
    ("unionDistinct",           CorpusRuntimeFixtures.unionDistinct,           CorpusMacroFixtures.unionDistinct),
    ("aliasSelect",             CorpusRuntimeFixtures.aliasSelect,             CorpusMacroFixtures.aliasSelect),
    ("offsetTail",              CorpusRuntimeFixtures.offsetTail,              CorpusMacroFixtures.offsetTail),
    ("orderByAsc",              CorpusRuntimeFixtures.orderByAsc,              CorpusMacroFixtures.orderByAsc),
    ("typedCols",               CorpusRuntimeFixtures.typedCols,               CorpusMacroFixtures.typedCols),
  )

  def spec = suite("EquivalenceSpec — runtime builder ≡ macro frontend (RelCodec render)")(
    extTableTests,
    sqlEntryPointTests,
    // original Warehouse fixtures (kept)
    sameRel("rates",                 RuntimeFixtures.rates,              MacroFixtures.rates),
    sameRel("daily_orders_by_state", RuntimeFixtures.dailyOrdersByState, MacroFixtures.dailyOrdersByState),
    sameRel("orders_enriched",       RuntimeFixtures.ordersEnriched,     MacroFixtures.ordersEnriched),
    sameRel("regions",               RuntimeFixtures.regions,            MacroFixtures.regions),
    // corpus parity (one named test per construct)
    suite("corpus parity")(corpus.map((n, r, m) => sameRel(n, r, m))*),
  )
