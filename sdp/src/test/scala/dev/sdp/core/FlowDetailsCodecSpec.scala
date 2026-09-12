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
        LineCodec.parseLine(line) == Right(LineCodec.ParsedLine.FlowLine(onceCdc)),
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

    // ------------------------------------------------------------ SCD2 (S2)

    test("v3 byte-compat: an SCD1 details render is EXACTLY the pre-SCD2 token stream") {
      // The frozen bytes. SCD2 adds two groups to the grammar, but they are
      // optional and trailing and are emitted only when non-empty, so every
      // manifest that does not use SCD2 — i.e. every manifest that exists —
      // renders unchanged: no header bump, no cache churn, no fragment skew.
      val minimal = FlowDetails.AutoCdc("src", List(Ex.Col("k")), Ex.Col("s"))
      assertTrue(
        FlowCodec.renderDetails(minimal) ==
          "autocdc src scd1 keys 1 %28col+k%29 seq %28col+s%29 " +
          "del 0 trunc 0 cols 0 except 0 ignidx 0 ignexc 0",
        // and the fully-populated SCD1 flow mentions neither new tag
        !FlowCodec.renderDetails(autoCdcDetails).contains(" track "),
        !FlowCodec.renderDetails(autoCdcDetails).contains(" trackexc "),
      )
    },

    test("an SCD2 flow round-trips, track-history lists and all") {
      val scd2 = autoCdcDetails.copy(
        scdType = ScdType.Scd2,
        trackHistoryColumnList = List(Ex.Col("name"), Ex.Col("tier")),
      )
      val excepted = autoCdcDetails.copy(
        scdType = ScdType.Scd2,
        trackHistoryExceptColumnList = List(Ex.Col("audit_ts")),
      )
      val bare = autoCdcDetails.copy(scdType = ScdType.Scd2)
      assertTrue(
        FlowCodec.renderDetails(scd2).contains(" scd2 "),
        FlowCodec.parseDetails(FlowCodec.renderDetails(scd2)) == Right(scd2),
        FlowCodec.parseDetails(FlowCodec.renderDetails(excepted)) == Right(excepted),
        // no track-history list at all is the common SCD2 shape ("track every
        // eligible column"), and it must not render an empty group
        FlowCodec.parseDetails(FlowCodec.renderDetails(bare)) == Right(bare),
        !FlowCodec.renderDetails(bare).contains("track"),
      )
    },

    test("an SCD2 manifest stays at sdp-manifest/3 and round-trips") {
      val scd2Flow = autoCdcFlow.copy(details =
        autoCdcDetails.copy(scdType = ScdType.Scd2, trackHistoryColumnList = List(Ex.Col("name")))
      )
      val m = manifestOf(scd2Flow)
      assertTrue(
        m.formatVersion == 3, // SCD2 is expressible in v3 — no new format
        PipelineManifest.parse(m.render) == Right(m),
      )
    },

    test("an unknown scd tag is a Left, not a throw (version skew names itself)") {
      val skewed = FlowCodec
        .renderDetails(FlowDetails.AutoCdc("src", List(Ex.Col("k")), Ex.Col("s")))
        .replace(" scd1 ", " scd3 ")
      assertTrue(FlowCodec.parseDetails(skewed) == Left("unknown scd type: scd3"))
    },

    test("Flow.reads of an AUTO CDC flow is exactly its source") {
      assertTrue(Flow.reads(autoCdcFlow) == Set("bronze.cdc"))
    },

    test("Flow.reads of a WriteRelation flow still reads its NamedTables") {
      assertTrue(Flow.reads(writeFlow) == Set("bronze.orders"))
    },
  )
