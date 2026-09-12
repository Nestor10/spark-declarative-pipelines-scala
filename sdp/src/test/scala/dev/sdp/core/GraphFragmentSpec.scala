package dev.sdp.core

import zio.test.*

object GraphFragmentSpec extends ZIOSpecDefault:

  private val fragment = GraphFragment(
    List(
      PipelineNode.StreamingTable("silver", "delta"),
      PipelineNode.MaterializedView("gold", "SELECT * FROM silver -- ünïcødé |%7C\nline2"),
    ),
    Set(DependencyEdge("bronze", "silver"), DependencyEdge("silver", "gold")),
  )

  def spec = suite("GraphFragment codec")(
    test("round-trip law: parse(render(f)) recovers nodes and edges") {
      val parsed = GraphFragment.parse(GraphFragment.render(fragment))
      assertTrue(
        parsed.map(_.nodes.toSet) == Right(fragment.nodes.toSet),
        parsed.map(_.edges) == Right(fragment.edges),
      )
    },
    test("render is canonical: node order quotiented away") {
      val reordered = fragment.copy(nodes = fragment.nodes.reverse)
      assertTrue(GraphFragment.render(fragment) == GraphFragment.render(reordered))
    },
    test("a malformed line is a Left naming what went wrong") {
      assertTrue(
        GraphFragment.parse("not a valid line").isLeft,
        GraphFragment.parse("node|bad%zz|table|delta").isLeft,
      )
    },
    test("empty fragment renders to the bare header and parses back") {
      assertTrue(
        GraphFragment.render(GraphFragment.empty) == "sdp-fragment/1",
        GraphFragment.parse("sdp-fragment/1") == Right(GraphFragment.empty),
        // the empty string is a pre-marker empty fragment — still the identity
        GraphFragment.parse("") == Right(GraphFragment.empty),
      )
    },
    suite("format marker (P3.1): version skew names itself")(
      test("render leads with the version header") {
        assertTrue(GraphFragment.render(fragment).linesIterator.next() == "sdp-fragment/1")
      },
      test("branch (a): a known version parses as usual") {
        val marked = GraphFragment.parse(GraphFragment.render(fragment))
        assertTrue(
          marked.map(_.nodes.toSet) == Right(fragment.nodes.toSet),
          marked.map(_.edges) == Right(fragment.edges),
        )
      },
      test("branch (b): a NEWER version is an error naming both versions") {
        val future = "sdp-fragment/2\n" + GraphFragment
          .render(fragment)
          .linesIterator
          .drop(1)
          .mkString("\n")
        val result = GraphFragment.parse(future)
        assertTrue(
          result.isLeft,
          result.swap.exists(_.contains("newer sdp (sdp-fragment/2)")),
          result.swap.exists(_.contains("this sdp understands sdp-fragment/1")),
          result.swap.exists(_.contains("lockstep")),
          // an unparseable or older marker is refused too, and still names both
          GraphFragment.parse("sdp-fragment/0").swap.exists(_.contains("sdp-fragment/1")),
          GraphFragment.parse("sdp-fragment/x").swap.exists(_.contains("unknown fragment format")),
        )
      },
      test("branch (c): an UNMARKED fragment (sdp <= 0.2.1) still parses") {
        // byte-for-byte what 0.2.1 wrote: the body with no header line
        val preMarker = GraphFragment.render(fragment).linesIterator.drop(1).mkString("\n")
        val parsed    = GraphFragment.parse(preMarker)
        assertTrue(
          !preMarker.startsWith("sdp-fragment/"),
          parsed.map(_.nodes.toSet) == Right(fragment.nodes.toSet),
          parsed.map(_.edges) == Right(fragment.edges),
        )
      },
      test("a body line can never be mistaken for a header") {
        // node/edge/flow lines are `|`-separated and percent-encoded; no
        // rendered line can start with the marker prefix
        assertTrue(
          GraphFragment
            .render(fragment)
            .linesIterator
            .drop(1)
            .forall(l => !l.startsWith("sdp-fragment/"))
        )
      },
    ),
    test("external table round-trips and satisfies reads without dangling") {
      // an external source `orders` read by a pipeline-managed `enriched`
      val ext = GraphFragment(
        List(
          PipelineNode.ExternalTable("main.bronze.orders"),
          PipelineNode.MaterializedView("enriched", "SELECT * FROM `main`.`bronze`.`orders`"),
        ),
        Set(DependencyEdge("main.bronze.orders", "enriched")),
      )
      val parsed = GraphFragment.parse(GraphFragment.render(ext))
      val graph  = PipelineGraph.fromFragments(ext.nodes, ext.edges)
      assertTrue(
        // codec preserves the external node + its qualified id
        parsed.map(_.nodes.toSet) == Right(ext.nodes.toSet),
        // the read of the external table is NOT dangling — it's a declared source
        graph.isRight,
        // and it sorts as a source (upstream of its consumer)
        graph.toOption.flatMap(_.topologicalSort.toOption).exists { order =>
          order.indexOf("main.bronze.orders") < order.indexOf("enriched")
        },
      )
    },
  )
