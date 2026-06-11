package dev.sdp.dsl

import dev.sdp.core.GraphFragment

/** The single, ordered list of (corpus-name, runtime fragment) pairs that both
  * [[GoldenGen]] (writes the golden file) and `GoldenRenderSpec` (checks against
  * it) consume — one source so the generator and the spec can never drift.
  *
  * The order matches the blocks in `golden-renders.txt`: the four Warehouse
  * fixtures first, then the construct corpus. Each name is the `<name>` half of
  * a `>>> <name>/<flowName>` golden key.
  */
object GoldenCorpus:

  /** Warehouse fixtures (RuntimeFixtures), each carrying exactly one flow. */
  val warehouse: List[(String, GraphFragment)] = List(
    "rates"                 -> RuntimeFixtures.rates,
    "daily_orders_by_state" -> RuntimeFixtures.dailyOrdersByState,
    "orders_enriched"       -> RuntimeFixtures.ordersEnriched,
    "regions"               -> RuntimeFixtures.regions,
  )

  /** Construct corpus (CorpusRuntimeFixtures), one entry per language feature. */
  val corpus: List[(String, GraphFragment)] = List(
    "sourceOptionWhereSelect" -> CorpusRuntimeFixtures.sourceOptionWhereSelect,
    "valIntermediates"        -> CorpusRuntimeFixtures.valIntermediates,
    "inlineToDf"              -> CorpusRuntimeFixtures.inlineToDf,
    "inlineMixed"             -> CorpusRuntimeFixtures.inlineMixed,
    "inlineSingleColumn"      -> CorpusRuntimeFixtures.inlineSingleColumn,
    "groupAggJoinOrderLimit"  -> CorpusRuntimeFixtures.groupAggJoinOrderLimit,
    "castSampleHint"          -> CorpusRuntimeFixtures.castSampleHint,
    "repartitionNaStat"       -> CorpusRuntimeFixtures.repartitionNaStat,
    "unpivotObserve"          -> CorpusRuntimeFixtures.unpivotObserve,
    "exprStarWindow"          -> CorpusRuntimeFixtures.exprStarWindow,
    "containerAccess"         -> CorpusRuntimeFixtures.containerAccess,
    "lambdasNamed"            -> CorpusRuntimeFixtures.lambdasNamed,
    "subqueries"              -> CorpusRuntimeFixtures.subqueries,
    "tastyRoundTrip"          -> CorpusRuntimeFixtures.tastyRoundTrip,
    "facadeTable"             -> CorpusRuntimeFixtures.facadeTable,
    "facadeSql"               -> CorpusRuntimeFixtures.facadeSql,
    "facadeRange1"            -> CorpusRuntimeFixtures.facadeRange1,
    "facadeRange3"            -> CorpusRuntimeFixtures.facadeRange3,
    "facadeReadTable"         -> CorpusRuntimeFixtures.facadeReadTable,
    "facadeReadStream"        -> CorpusRuntimeFixtures.facadeReadStream,
    "facadeStreamSource"      -> CorpusRuntimeFixtures.facadeStreamSource,
    "facadeBatchSource"       -> CorpusRuntimeFixtures.facadeBatchSource,
    "guideExample"            -> CorpusRuntimeFixtures.guideExample,
    "sugarStringsSortFilter"  -> CorpusRuntimeFixtures.sugarStringsSortFilter,
    "sugarIsin"               -> CorpusRuntimeFixtures.sugarIsin,
    "sugarNaFill"             -> CorpusRuntimeFixtures.sugarNaFill,
    "sugarStatCorr"           -> CorpusRuntimeFixtures.sugarStatCorr,
    "withColumnsMaps"         -> CorpusRuntimeFixtures.withColumnsMaps,
    "functionsFacade"         -> CorpusRuntimeFixtures.functionsFacade,
    "hofsNoWrapper"           -> CorpusRuntimeFixtures.hofsNoWrapper,
    "ddlSchema"               -> CorpusRuntimeFixtures.ddlSchema,
    "withSchemaSource"        -> CorpusRuntimeFixtures.withSchemaSource,
    "crossModulo"             -> CorpusRuntimeFixtures.crossModulo,
    "booleanAnd"              -> CorpusRuntimeFixtures.booleanAnd,
    "dropDuplicatesOne"       -> CorpusRuntimeFixtures.dropDuplicatesOne,
    "joinDrop"                -> CorpusRuntimeFixtures.joinDrop,
    "unionDistinct"           -> CorpusRuntimeFixtures.unionDistinct,
    "aliasSelect"             -> CorpusRuntimeFixtures.aliasSelect,
    "offsetTail"              -> CorpusRuntimeFixtures.offsetTail,
    "orderByAsc"              -> CorpusRuntimeFixtures.orderByAsc,
    "typedCols"               -> CorpusRuntimeFixtures.typedCols,
  )

  /** Warehouse then corpus, in golden-file block order. */
  val all: List[(String, GraphFragment)] = warehouse ++ corpus
