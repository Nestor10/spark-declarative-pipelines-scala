package dev.sdp.connect.app

import dev.sdp.core.*
import zio.test.*

/** Task-3 smoke: the offline subcommand logic, tested as ZIO values (no JVM
  * process, no container). A valid pipeline assembles; a pipeline with a
  * dangling read fails in the typed channel with the same error rendering the
  * sbt plugin emits (Zionomicon ch. 3: expected errors are values).
  *
  * Fragments are built straight from the core ADTs (the DSL macros produce
  * the same `GraphFragment` shapes) so the test needs nothing but sdp-core.
  */
object SdpAppSpec extends ZIOSpecDefault:

  /** Two-fragment valid pipeline: a source table and a streaming table that
    * reads it. The edge resolves because both endpoints are declared. */
  private val validPipeline: List[GraphFragment] = List(
    GraphFragment(List(PipelineNode.Table("bronze", "delta")), Set.empty),
    GraphFragment(
      List(PipelineNode.StreamingTable("silver", "delta")),
      Set(DependencyEdge("bronze", "silver")),
    ),
  )

  /** A pipeline whose lineage references an undeclared upstream ("ghost") —
    * the validator must surface a DanglingEdges error. */
  private val danglingPipeline: List[GraphFragment] = List(
    GraphFragment(
      List(PipelineNode.StreamingTable("silver", "delta")),
      Set(DependencyEdge("ghost", "silver")),
    )
  )

  def spec = suite("SdpApp offline command logic")(
    test("validate: a sound 2-fragment pipeline succeeds") {
      SdpCommands.validate(validPipeline).exit.map { exit =>
        assertTrue(exit.isSuccess)
      }
    },
    test("manifest: a sound pipeline renders a parseable .sdpm manifest") {
      for
        text <- SdpCommands.manifest(validPipeline)
      yield assertTrue(
        text.startsWith("sdp-manifest/"),
        PipelineManifest.parse(text).isRight,
      )
    },
    test("validate: a dangling read fails with the rendered DanglingEdges error") {
      SdpCommands.validate(danglingPipeline).either.map {
        case Left(err: SdpCommands.CommandError.Invalid) =>
          val rendered = err.render
          assertTrue(
            err.errors.exists {
              case PipelineValidationError.DanglingEdges(ids) => ids.contains("ghost")
              case _                                          => false
            },
            rendered.contains("SDP pipeline graph is invalid"),
            rendered.contains("ghost"),
          )
        case other =>
          assertTrue(false) ?? s"expected CommandError.Invalid, got $other"
      }
    },
  )
