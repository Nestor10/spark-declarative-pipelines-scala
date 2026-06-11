package dev.sdp.app

import dev.sdp.core.*
import zio.test.*

import PipelineValidationError.*

object GraphValidationSpec extends ZIOSpecDefault:

  private def tbl(id: String): PipelineNode = PipelineNode.Table(id, "delta")

  def spec = suite("GraphValidation")(
    test("succeeds with the constructed graph for valid fragments") {
      for graph <- GraphValidation.validate(
          List(tbl("bronze"), tbl("silver")),
          Set(DependencyEdge("bronze", "silver")),
        )
      yield assertTrue(graph.nodes.keySet == Set("bronze", "silver"))
    },
    test("fails with every accumulated error in the typed channel") {
      for exit <- GraphValidation
          .validate(
            List(tbl("dup"), tbl("dup"), tbl("x"), tbl("y")),
            Set(DependencyEdge("x", "y"), DependencyEdge("y", "x"), DependencyEdge("x", "ghost")),
          )
          .exit
      yield assertTrue(
        exit.causeOption
          .flatMap(_.failureOption)
          .exists { errors =>
            errors.size == 3 &&
            errors.contains(DuplicateNode("dup")) &&
            errors.contains(DanglingEdges(List("ghost"))) &&
            errors.exists { case CycleDetected(_) => true; case _ => false }
          }
      )
    },
    test("the error channel is non-empty by type: a failure always has a head") {
      for exit <- GraphValidation
          .validate(List(tbl("a")), Set(DependencyEdge("a", "missing")))
          .exit
      yield assertTrue(
        // `.head` on `::[E]` is total — that's the point of the cons type.
        exit.causeOption.flatMap(_.failureOption).map(_.head) ==
          Some(DanglingEdges(List("missing")))
      )
    },
  ).provide(GraphValidation.live)
