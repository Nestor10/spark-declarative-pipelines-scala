package dev.sdp.app

import dev.sdp.core.*
import zio.test.*

import PipelineValidationError.*

object ManifestAssemblySpec extends ZIOSpecDefault:

  private def tbl(id: String): PipelineNode = PipelineNode.Table(id, "delta")

  // Two "modules": one declares the bronze layer, the other builds on it.
  private val moduleA = GraphFragment(List(tbl("bronze_orders")), Set.empty)
  private val moduleB = GraphFragment(
    List(PipelineNode.StreamingTable("silver_orders", "delta")),
    Set(DependencyEdge("bronze_orders", "silver_orders")),
  )

  def spec = suite("ManifestAssembly")(
    test("disjoint fragments from two modules merge into one valid manifest") {
      for manifest <- ManifestAssembly.assemble(List(moduleA, moduleB))
      yield assertTrue(
        manifest.nodes.map(_.id) == List("bronze_orders", "silver_orders"),
        manifest.edges == List(DependencyEdge("bronze_orders", "silver_orders")),
      )
    },
    test("assembly is order-independent: [A, B] and [B, A] yield identical manifests") {
      for
        ab <- ManifestAssembly.assemble(List(moduleA, moduleB))
        ba <- ManifestAssembly.assemble(List(moduleB, moduleA))
      yield assertTrue(ab == ba, ab.render == ba.render)
    },
    test("two modules declaring the same dataset id surface DuplicateNode") {
      val conflicting = GraphFragment(List(tbl("bronze_orders")), Set.empty)
      for exit <- ManifestAssembly.assemble(List(moduleA, conflicting)).exit
      yield assertTrue(
        exit.causeOption.flatMap(_.failureOption) ==
          Some(::(DuplicateNode("bronze_orders"), Nil))
      )
    },
    test("a cross-module cycle is caught after the merge") {
      val up   = GraphFragment(List(tbl("a")), Set(DependencyEdge("b", "a")))
      val down = GraphFragment(List(tbl("b")), Set(DependencyEdge("a", "b")))
      for exit <- ManifestAssembly.assemble(List(up, down)).exit
      yield assertTrue(
        exit.causeOption.flatMap(_.failureOption).exists(_.exists {
          case CycleDetected(_) => true
          case _                => false
        })
      )
    },
    test("the empty assembly is a valid empty manifest") {
      for manifest <- ManifestAssembly.assemble(Nil)
      yield assertTrue(manifest.nodes.isEmpty, manifest.edges.isEmpty)
    },
    test("flow reads become derived lineage edges in the manifest") {
      import dev.sdp.core.algebra.*
      val fragment = GraphFragment(
        List(tbl("src"), PipelineNode.StreamingTable("out", "delta")),
        Set.empty,
        List(Flow("out", "out", Rel.Filter(
          Rel.NamedTable("src", streaming = true),
          Ex.Fn(">", List(Ex.Col("id"), Ex.Lit(LitValue.I32(0)))),
        ))),
      )
      for manifest <- ManifestAssembly.assemble(List(fragment))
      yield assertTrue(
        manifest.edges == List(DependencyEdge("src", "out")),
        manifest.flows.map(_.name) == List("out"),
      )
    },
    test("a flow whose reads create a cycle is caught at assembly time") {
      import dev.sdp.core.algebra.*
      val fragment = GraphFragment(
        List(PipelineNode.StreamingTable("a", "delta"), PipelineNode.StreamingTable("b", "delta")),
        Set.empty,
        List(
          Flow("a", "a", Rel.NamedTable("b", streaming = true)),
          Flow("b", "b", Rel.NamedTable("a", streaming = true)),
        ),
      )
      for exit <- ManifestAssembly.assemble(List(fragment)).exit
      yield assertTrue(
        exit.causeOption.flatMap(_.failureOption).exists(_.exists {
          case CycleDetected(_) => true
          case _                => false
        })
      )
    },
    test("a valid AUTO CDC flow into a streaming table assembles into a v3 manifest") {
      import dev.sdp.core.algebra.*
      val fragment = GraphFragment(
        List(
          PipelineNode.ExternalTable("bronze.cdc"),
          PipelineNode.StreamingTable("dim", "delta"),
        ),
        Set.empty,
        List(Flow(
          "dim_auto_cdc",
          "dim",
          FlowDetails.AutoCdc(
            source = "bronze.cdc",
            keys = List(Ex.Col("id")),
            sequenceBy = Ex.Col("seq"),
          ),
        )),
      )
      for manifest <- ManifestAssembly.assemble(List(fragment))
      yield assertTrue(
        manifest.formatVersion == 3,
        // source becomes a derived lineage edge into the target
        manifest.edges == List(DependencyEdge("bronze.cdc", "dim")),
        manifest.flows.map(_.name) == List("dim_auto_cdc"),
      )
    },
    test("AUTO CDC into a non-streaming-table target is rejected") {
      import dev.sdp.core.algebra.*
      val fragment = GraphFragment(
        List(tbl("src"), tbl("dim")), // dim is a batch Table, not streaming
        Set.empty,
        List(Flow(
          "dim_auto_cdc", "dim",
          FlowDetails.AutoCdc("src", List(Ex.Col("id")), Ex.Col("seq")),
        )),
      )
      for exit <- ManifestAssembly.assemble(List(fragment)).exit
      yield assertTrue(
        exit.causeOption.flatMap(_.failureOption).exists(_.exists {
          case AutoCdcTargetNotStreamingTable("dim_auto_cdc", "dim") => true
          case _                                                     => false
        })
      )
    },
    test("AUTO CDC with empty keys is rejected") {
      import dev.sdp.core.algebra.*
      val fragment = GraphFragment(
        List(tbl("src"), PipelineNode.StreamingTable("dim", "delta")),
        Set.empty,
        List(Flow(
          "dim_auto_cdc", "dim",
          FlowDetails.AutoCdc("src", Nil, Ex.Col("seq")),
        )),
      )
      for exit <- ManifestAssembly.assemble(List(fragment)).exit
      yield assertTrue(
        exit.causeOption.flatMap(_.failureOption).exists(_.exists {
          case AutoCdcKeysEmpty("dim_auto_cdc") => true
          case _                                => false
        })
      )
    },
    test("AUTO CDC with a missing source is caught by the dangling-read rule") {
      import dev.sdp.core.algebra.*
      val fragment = GraphFragment(
        List(PipelineNode.StreamingTable("dim", "delta")), // no source declared
        Set.empty,
        List(Flow(
          "dim_auto_cdc", "dim",
          FlowDetails.AutoCdc("ghost.cdc", List(Ex.Col("id")), Ex.Col("seq")),
        )),
      )
      for exit <- ManifestAssembly.assemble(List(fragment)).exit
      yield assertTrue(
        exit.causeOption.flatMap(_.failureOption).exists(_.exists {
          case DanglingEdges(ids) => ids.contains("ghost.cdc")
          case _                  => false
        })
      )
    },
    test("a flow targeting an undeclared dataset fails, accumulated with graph errors") {
      import dev.sdp.core.algebra.*
      val fragment = GraphFragment(
        List(tbl("src")),
        Set(DependencyEdge("ghost", "src")), // dangling, too
        List(Flow("f1", "nowhere", Rel.NamedTable("src", streaming = false))),
      )
      for exit <- ManifestAssembly.assemble(List(fragment)).exit
      yield assertTrue(
        exit.causeOption.flatMap(_.failureOption).exists { errors =>
          errors.contains(UnknownFlowTarget("f1", "nowhere")) &&
          errors.exists { case DanglingEdges(_) => true; case _ => false }
        }
      )
    },
  ).provide(ManifestAssembly.live, GraphValidation.live)
