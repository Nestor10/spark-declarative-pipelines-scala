package dev.sdp.dsl.runtime

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}

import dev.sdp.core.GraphFragment
import dev.sdp.core.algebra.RelCodec

/** One-shot generator: freezes the MACRO frontend's `RelCodec.render` output
  * for the whole parity corpus into `src/test/resources/golden-renders.txt`.
  *
  * Run ONCE before the macro frontend is deleted (M3); after deletion the
  * corpus equivalence spec compares the runtime builder against this file
  * instead of against live macro extraction — same oracle, frozen.
  *
  * {{{ sbt 'sdpRuntimeDsl/Test/runMain dev.sdp.dsl.runtime.GoldenGen' }}}
  *
  * Format: one block per construct —
  * `>>> <name>/<flowName>` line, the render on the following line(s), blank
  * line between blocks. Renders are stable by construction (D6: render →
  * parseTrusted is the canonical round-trip the manifest itself uses).
  */
object GoldenGen:

  private def blocks(name: String, frag: GraphFragment): List[String] =
    frag.flows.map(f => s">>> $name/${f.name}\n${RelCodec.render(f.relation)}")

  def main(args: Array[String]): Unit =
    val warehouse = List(
      ("rates", MacroFixtures.rates),
      ("daily_orders_by_state", MacroFixtures.dailyOrdersByState),
      ("orders_enriched", MacroFixtures.ordersEnriched),
      ("regions", MacroFixtures.regions),
    )
    val corpus = List(
      ("sourceOptionWhereSelect", CorpusMacroFixtures.sourceOptionWhereSelect),
      ("valIntermediates", CorpusMacroFixtures.valIntermediates),
      ("inlineToDf", CorpusMacroFixtures.inlineToDf),
      ("inlineMixed", CorpusMacroFixtures.inlineMixed),
      ("inlineSingleColumn", CorpusMacroFixtures.inlineSingleColumn),
      ("groupAggJoinOrderLimit", CorpusMacroFixtures.groupAggJoinOrderLimit),
      ("castSampleHint", CorpusMacroFixtures.castSampleHint),
      ("repartitionNaStat", CorpusMacroFixtures.repartitionNaStat),
      ("unpivotObserve", CorpusMacroFixtures.unpivotObserve),
      ("exprStarWindow", CorpusMacroFixtures.exprStarWindow),
      ("containerAccess", CorpusMacroFixtures.containerAccess),
      ("lambdasNamed", CorpusMacroFixtures.lambdasNamed),
      ("subqueries", CorpusMacroFixtures.subqueries),
      ("tastyRoundTrip", CorpusMacroFixtures.tastyRoundTrip),
      ("facadeTable", CorpusMacroFixtures.facadeTable),
      ("facadeSql", CorpusMacroFixtures.facadeSql),
      ("facadeRange1", CorpusMacroFixtures.facadeRange1),
      ("facadeRange3", CorpusMacroFixtures.facadeRange3),
      ("facadeReadTable", CorpusMacroFixtures.facadeReadTable),
      ("facadeReadStream", CorpusMacroFixtures.facadeReadStream),
      ("facadeStreamSource", CorpusMacroFixtures.facadeStreamSource),
      ("facadeBatchSource", CorpusMacroFixtures.facadeBatchSource),
      ("guideExample", CorpusMacroFixtures.guideExample),
      ("sugarStringsSortFilter", CorpusMacroFixtures.sugarStringsSortFilter),
      ("sugarIsin", CorpusMacroFixtures.sugarIsin),
      ("sugarNaFill", CorpusMacroFixtures.sugarNaFill),
      ("sugarStatCorr", CorpusMacroFixtures.sugarStatCorr),
      ("withColumnsMaps", CorpusMacroFixtures.withColumnsMaps),
      ("functionsFacade", CorpusMacroFixtures.functionsFacade),
      ("hofsNoWrapper", CorpusMacroFixtures.hofsNoWrapper),
      ("ddlSchema", CorpusMacroFixtures.ddlSchema),
      ("withSchemaSource", CorpusMacroFixtures.withSchemaSource),
      ("crossModulo", CorpusMacroFixtures.crossModulo),
      ("booleanAnd", CorpusMacroFixtures.booleanAnd),
      ("dropDuplicatesOne", CorpusMacroFixtures.dropDuplicatesOne),
      ("joinDrop", CorpusMacroFixtures.joinDrop),
      ("unionDistinct", CorpusMacroFixtures.unionDistinct),
      ("aliasSelect", CorpusMacroFixtures.aliasSelect),
      ("offsetTail", CorpusMacroFixtures.offsetTail),
      ("orderByAsc", CorpusMacroFixtures.orderByAsc),
      ("typedCols", CorpusMacroFixtures.typedCols),
    )
    val out  = (warehouse ++ corpus).flatMap(blocks).mkString("", "\n\n", "\n")
    // sbt 2 runs Test/runMain with the MODULE directory as CWD.
    val path = Paths.get("src/test/resources/golden-renders.txt")
    Files.createDirectories(path.getParent)
    val _ = Files.write(path, out.getBytes(UTF_8))
    println(s"wrote ${path.toAbsolutePath} (${out.linesIterator.count(_.startsWith(">>>"))} flows)")
