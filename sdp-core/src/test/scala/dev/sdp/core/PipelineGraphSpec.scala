package dev.sdp.core

import zio.test.*

import PipelineValidationError.*

object PipelineGraphSpec extends ZIOSpecDefault:

  private def tbl(id: String): PipelineNode = PipelineNode.Table(id, "delta")

  private def graph(ids: List[String], edges: (String, String)*): PipelineGraph =
    PipelineGraph(
      ids.map(id => id -> tbl(id)).toMap,
      edges.map((f, t) => DependencyEdge(f, t)).toSet,
    )

  def spec = suite("PipelineGraph")(
    suite("fromFragments")(
      test("accepts a valid linear graph") {
        val result = PipelineGraph.fromFragments(
          List(tbl("bronze"), tbl("silver"), tbl("gold")),
          Set(DependencyEdge("bronze", "silver"), DependencyEdge("silver", "gold")),
        )
        assertTrue(result.isRight)
      },
      test("reports duplicate node ids, sorted") {
        val result = PipelineGraph.fromFragments(
          List(tbl("b"), tbl("a"), tbl("b"), tbl("a")),
          Set.empty,
        )
        assertTrue(result == Left(::(DuplicateNode("a"), List(DuplicateNode("b")))))
      },
      test("accumulates duplicates, dangling edges, and cycles in one pass") {
        val result = PipelineGraph.fromFragments(
          List(tbl("dup"), tbl("dup"), tbl("x"), tbl("y")),
          Set(
            DependencyEdge("x", "y"),
            DependencyEdge("y", "x"),     // cycle
            DependencyEdge("x", "ghost"), // dangling
          ),
        )
        val errors = result.swap.getOrElse(Nil)
        assertTrue(
          errors.size == 3,
          errors.contains(DuplicateNode("dup")),
          errors.contains(DanglingEdges(List("ghost"))),
          errors.exists { case CycleDetected(_) => true; case _ => false },
        )
      },
    ),
    suite("validate")(
      test("an empty graph is valid") {
        assertTrue(graph(Nil).validate.isEmpty)
      },
      test("reports dangling edge endpoints, deduplicated and sorted") {
        val g = graph(List("a"), ("a", "zz"), ("qq", "a"), ("qq", "zz"))
        assertTrue(g.validate == List(DanglingEdges(List("qq", "zz"))))
      },
      test("reports a cycle as a closed path") {
        val g = graph(List("a", "b", "c"), ("a", "b"), ("b", "c"), ("c", "a"))
        assertTrue(g.validate == List(CycleDetected(List("a", "b", "c", "a"))))
      },
      test("reports a self-loop as a minimal closed path") {
        val g = graph(List("solo"), ("solo", "solo"))
        assertTrue(g.validate == List(CycleDetected(List("solo", "solo"))))
      },
      test("a diamond is not a cycle") {
        val g = graph(List("a", "b", "c", "d"), ("a", "b"), ("a", "c"), ("b", "d"), ("c", "d"))
        assertTrue(g.validate.isEmpty)
      },
    ),
    suite("topologicalSort")(
      test("every edge is respected: from precedes to") {
        val g = graph(List("a", "b", "c", "d"), ("a", "b"), ("a", "c"), ("b", "d"), ("c", "d"))
        val Right(order) = g.topologicalSort: @unchecked
        val index        = order.zipWithIndex.toMap
        assertTrue(
          g.edges.forall(e => index(e.from) < index(e.to)),
          order.toSet == g.nodes.keySet,
        )
      },
      test("is deterministic regardless of fragment insertion order") {
        val ids   = List("m", "a", "z", "k", "b")
        val edges = Set(DependencyEdge("z", "a"), DependencyEdge("m", "k"))
        val g1    = PipelineGraph(ids.map(id => id -> tbl(id)).toMap, edges)
        val g2    = PipelineGraph(ids.reverse.map(id => id -> tbl(id)).toMap, edges)
        assertTrue(g1.topologicalSort == g2.topologicalSort)
      },
      test("fails with the cycle path on a cyclic graph") {
        val g = graph(List("a", "b"), ("a", "b"), ("b", "a"))
        assertTrue(g.topologicalSort == Left(CycleDetected(List("a", "b", "a"))))
      },
      test("ignores phantom ids introduced by dangling edges") {
        val g            = graph(List("a"), ("a", "ghost"))
        val Right(order) = g.topologicalSort: @unchecked
        assertTrue(order == List("a"))
      },
    ),
  )
