package dev.sdp.core

import dev.sdp.core.algebra.*
import zio.test.*

/** AUTO CDC domain + codec coverage:
  *   - manifest v2 byte-for-byte unchanged for old constructs (WriteRelation +
  *     once=false) — the frozen-contract guarantee;
  *   - manifest header bumps to v3 ONLY when a v3 construct is present;
  *   - FlowDetails / Flow round-trip through the line codec (v2 and v3);
  *   - the [[Flow.reads]] of an AUTO CDC flow is exactly its source.
  */
object FlowDetailsCodecSpec extends ZIOSpecDefault:

  private val writeFlow =
    Flow("silver", "silver", Rel.NamedTable("bronze.orders", streaming = true))

  // Ascribed to the case type (not the enum supertype) so .copy is available.
  private val autoCdcDetails: FlowDetails.AutoCdc = FlowDetails.AutoCdc(
    source = "bronze.cdc",
    keys = List(Ex.Col("id"), Ex.Col("region")),
    sequenceBy = Ex.Col("seq"),
    applyAsDeletes = Some(Ex.Fn("==", List(Ex.Col("op"), Ex.Lit(LitValue.Str("DELETE"))))),
    applyAsTruncates = None,
    columnList = List(Ex.Col("id"), Ex.Col("name")),
    exceptColumnList = Nil,
    ignoreNullUpdatesColumnList = List(Ex.Col("name")),
    ignoreNullUpdatesExceptColumnList = Nil,
    scdType = ScdType.Scd1,
  )

  private val autoCdcFlow = Flow("dim_auto_cdc", "dim", autoCdcDetails)

  /** Render a one-flow manifest. */
  private def manifestOf(flow: Flow): PipelineManifest =
    val graph = PipelineGraph(
      Map("silver" -> PipelineNode.StreamingTable("silver", "delta")),
      Set.empty,
    )
    PipelineManifest.fromGraphAndFlows(graph, List(flow))

  def spec = suite("FlowDetailsCodec / manifest v2-compat + v3")(

    test("v2 byte-compat: a WriteRelation/once=false flow renders to the exact v2 line") {
      // The exact bytes a pre-v3 build wrote for this flow:
      val expectedLine =
        s"flow|${LineCodec.enc("silver")}|${LineCodec.enc("silver")}|" +
          LineCodec.enc(RelCodec.render(writeFlow.relation))
      assertTrue(LineCodec.renderFlow(writeFlow) == expectedLine)
    },

    test("a graph with no v3 construct still renders header sdp-manifest/2") {
      val m = manifestOf(writeFlow)
      assertTrue(m.formatVersion == 2, m.render.linesIterator.next() == "sdp-manifest/2")
    },

    test("v2 manifest round-trips: parse(render) == Right(m)") {
      val m = manifestOf(writeFlow)
      assertTrue(PipelineManifest.parse(m.render) == Right(m))
    },

    test("an AUTO CDC flow bumps the header to sdp-manifest/3") {
      val m = manifestOf(autoCdcFlow)
      assertTrue(m.formatVersion == 3, m.render.linesIterator.next() == "sdp-manifest/3")
    },

    test("a once=true WriteRelation flow bumps the header to sdp-manifest/3") {
      val onceFlow = Flow("t", "t", FlowDetails.WriteRelation(Rel.NamedTable("u", streaming = false)), once = true)
      val m = manifestOf(onceFlow)
      assertTrue(m.formatVersion == 3)
    },

    test("FlowDetails.AutoCdc round-trips through FlowCodec") {
      val rendered = FlowCodec.renderDetails(autoCdcDetails)
      assertTrue(FlowCodec.parseDetails(rendered) == Right(autoCdcDetails))
    },

    test("a fully-populated AutoCdc (truncates + except + ignexc) round-trips") {
      val full = autoCdcDetails.copy(
        applyAsTruncates = Some(Ex.Col("trunc")),
        exceptColumnList = List(Ex.Col("audit")),
        ignoreNullUpdatesExceptColumnList = List(Ex.Col("ts")),
      )
      assertTrue(FlowCodec.parseDetails(FlowCodec.renderDetails(full)) == Right(full))
    },

    test("a minimal AutoCdc (no optionals, single key) round-trips") {
      val minimal = FlowDetails.AutoCdc(
        source = "src",
        keys = List(Ex.Col("k")),
        sequenceBy = Ex.Col("s"),
      )
      assertTrue(FlowCodec.parseDetails(FlowCodec.renderDetails(minimal)) == Right(minimal))
    },

    test("a v3 flow line (AutoCdc + once) round-trips through the line codec") {
      val onceCdc = autoCdcFlow.copy(once = true)
      val line    = LineCodec.renderFlow(onceCdc)
      assertTrue(
        line.split("\\|", -1).length == 5, // five-field v3 line
        LineCodec.parseLine(line) == Some(LineCodec.ParsedLine.FlowLine(onceCdc)),
      )
    },

    test("a v3 manifest with mixed flows round-trips") {
      val graph = PipelineGraph(
        Map(
          "silver" -> PipelineNode.StreamingTable("silver", "delta"),
          "dim"    -> PipelineNode.StreamingTable("dim", "delta"),
        ),
        Set.empty,
      )
      val m = PipelineManifest.fromGraphAndFlows(graph, List(writeFlow, autoCdcFlow))
      assertTrue(m.formatVersion == 3, PipelineManifest.parse(m.render) == Right(m))
    },

    test("Flow.reads of an AUTO CDC flow is exactly its source") {
      assertTrue(Flow.reads(autoCdcFlow) == Set("bronze.cdc"))
    },

    test("Flow.reads of a WriteRelation flow still reads its NamedTables") {
      assertTrue(Flow.reads(writeFlow) == Set("bronze.orders"))
    },
  )
