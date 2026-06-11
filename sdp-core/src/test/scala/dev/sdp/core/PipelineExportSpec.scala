package dev.sdp.core

import dev.sdp.core.algebra.Rel
import zio.test.*

/** `PipelineExport.encodeAll` is the cross-classloader boundary: it must render
  * every fragment with the SAME string codec the plugin decodes with
  * (`GraphFragment.parse`), so `parse(encodeAll(...)(i))` recovers each fragment.
  */
object PipelineExportSpec extends ZIOSpecDefault:

  private val external = GraphFragment(
    List(PipelineNode.ExternalTable("bronze.orders")),
    Set.empty,
  )

  private val withFlow = GraphFragment(
    List(PipelineNode.StreamingTable("silver", "delta")),
    Set.empty,
    List(Flow("silver", "silver", Rel.NamedTable("bronze.orders", streaming = true))),
  )

  private val pipeline: List[GraphFragment] = List(external, withFlow)

  def spec = suite("PipelineExport.encodeAll")(
    test("encodes a List[GraphFragment] one string per fragment, decodable by parse") {
      val lines = PipelineExport.encodeAll(pipeline)
      val decoded = lines.toList.map(GraphFragment.parse)
      assertTrue(
        lines.length == 2,
        decoded == List(Right(external), Right(withFlow)),
      )
    },
    test("each string equals GraphFragment.render of the corresponding fragment") {
      val lines = PipelineExport.encodeAll(pipeline)
      assertTrue(
        lines.toList == pipeline.map(GraphFragment.render),
      )
    },
    test("accepts a single GraphFragment as a one-element pipeline") {
      val lines = PipelineExport.encodeAll(external)
      assertTrue(
        lines.length == 1,
        GraphFragment.parse(lines(0)) == Right(external),
      )
    },
    test("a non-fragment value fails loudly") {
      val result = scala.util.Try(PipelineExport.encodeAll("not a pipeline"))
      assertTrue(result.isFailure)
    },
    test("a collection holding a non-fragment fails loudly") {
      val result = scala.util.Try(PipelineExport.encodeAll(List(external, "oops")))
      assertTrue(result.isFailure)
    },
  )
