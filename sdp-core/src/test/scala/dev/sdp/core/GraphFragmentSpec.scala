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
    test("parseTrusted dies on malformed input with a bug-attribution message") {
      val result = scala.util.Try(GraphFragment.parseTrusted("not a valid line"))
      assertTrue(
        result.failed.toOption.exists(_.getMessage.contains("bug in sdp-runtime-dsl"))
      )
    },
    test("empty fragment renders to the empty string and parses back") {
      assertTrue(
        GraphFragment.render(GraphFragment.empty) == "",
        GraphFragment.parse("") == Right(GraphFragment.empty),
      )
    },
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
