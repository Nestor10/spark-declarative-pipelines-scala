package dev.sdp.core

import zio.test.*

object PipelineManifestSpec extends ZIOSpecDefault:

  private val sampleGraph = PipelineGraph(
    Map(
      "bronze" -> PipelineNode.Table("bronze", "delta"),
      "silver" -> PipelineNode.StreamingTable("silver", "delta"),
      "gold"   -> PipelineNode.MaterializedView("gold", "SELECT id, count(*) FROM silver GROUP BY id"),
      "scratch" -> PipelineNode.TemporaryView("scratch", "SELECT * FROM bronze WHERE x > 1"),
    ),
    Set(
      DependencyEdge("bronze", "silver"),
      DependencyEdge("silver", "gold"),
      DependencyEdge("bronze", "scratch"),
    ),
  )

  def spec = suite("PipelineManifest")(
    test("round-trip law: parse(render(m)) == Right(m)") {
      val m = PipelineManifest.fromGraph(sampleGraph)
      assertTrue(PipelineManifest.parse(m.render) == Right(m))
    },
    test("rendering is byte-stable across structurally equal graphs built in different orders") {
      val reordered = PipelineGraph(
        sampleGraph.nodes.toList.reverse.toMap,
        sampleGraph.edges.toList.reverse.toSet,
      )
      assertTrue(
        PipelineManifest.fromGraph(sampleGraph).render ==
          PipelineManifest.fromGraph(reordered).render
      )
    },
    test("fields survive hostile characters: pipes, newlines, unicode, percent signs") {
      val hostile = PipelineGraph(
        Map(
          "we|ird"  -> PipelineNode.MaterializedView("we|ird", "SELECT '\n%|7C' AS s -- ünïcødé"),
          "plain"   -> PipelineNode.Table("plain", "delta"),
        ),
        Set(DependencyEdge("plain", "we|ird")),
      )
      val m = PipelineManifest.fromGraph(hostile)
      assertTrue(PipelineManifest.parse(m.render) == Right(m))
    },
    test("toGraph rehydrates the original graph") {
      val m = PipelineManifest.fromGraph(sampleGraph)
      assertTrue(m.toGraph == sampleGraph)
    },
    test("parse canonicalizes a hand-shuffled body") {
      val m        = PipelineManifest.fromGraph(sampleGraph)
      val lines    = m.render.linesIterator.toList
      val shuffled = (lines.head :: lines.tail.reverse).mkString("", "\n", "\n")
      assertTrue(PipelineManifest.parse(shuffled) == Right(m))
    },
    test("rejects a missing header") {
      assertTrue(PipelineManifest.parse("") == Left(PipelineManifest.ParseError.MissingHeader))
    },
    test("rejects an unsupported version") {
      assertTrue(
        PipelineManifest.parse("sdp-manifest/99\n") ==
          Left(PipelineManifest.ParseError.UnsupportedVersion("99"))
      )
    },
    test("rejects a malformed line with its 1-based location and a diagnostic") {
      val text = "sdp-manifest/1\nnode|a|table|delta\ngarbage line\n"
      assertTrue(
        PipelineManifest.parse(text) ==
          Left(
            PipelineManifest.ParseError.MalformedLine(
              3,
              "garbage line",
              "unrecognized line (1 fields): 'garbage line'",
            )
          )
      )
    },
    test("a malformed percent-escape is an error, not a throw, and names the field") {
      // %zz is not a valid escape: URLDecoder throws, the codec must not.
      val badNode = "sdp-manifest/2\nnode|bad%zz|table|delta\n"
      val badFlow = "sdp-manifest/2\nnode|a|table|delta\nflow|f|a|%28read+t+batch%zz\n"
      val nodeDetail = PipelineManifest.parse(badNode) match
        case Left(PipelineManifest.ParseError.MalformedLine(2, _, detail)) => detail
        case other                                                        => s"unexpected: $other"
      assertTrue(
        nodeDetail.contains("malformed percent-encoding"),
        nodeDetail.contains("bad%zz"),
        PipelineManifest.parse(badFlow).isLeft,
      )
    },
  )
