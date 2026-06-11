package dev.sdp.app

import dev.sdp.core.{Flow, GraphFragment, PipelineManifest, PipelineValidationError}
import zio.*

/** Assembles per-compilation-unit fragments into the canonical manifest.
  *
  * This is the exact entry point the sbt task calls: macro-emitted fragments
  * in, validated byte-stable manifest out. The fragment merge is a pure
  * monoid fold (order-independent by construction); validation happens once,
  * after the merge, where cross-module conflicts are observable.
  *
  * Flows participate in validation two ways:
  *   - their relation's `NamedTable` reads become derived lineage edges, so
  *     cycle/dangling detection sees what flows *actually* read (SQL bodies
  *     are opaque — those resolve server-side);
  *   - every flow's target must be a declared dataset
  *     ([[PipelineValidationError.UnknownFlowTarget]]), accumulated alongside
  *     graph errors, not short-circuiting them.
  */
trait ManifestAssembly:
  def assemble(fragments: List[GraphFragment]): IO[::[PipelineValidationError], PipelineManifest]

object ManifestAssembly:

  def assemble(
      fragments: List[GraphFragment]
  ): ZIO[ManifestAssembly, ::[PipelineValidationError], PipelineManifest] =
    ZIO.serviceWithZIO[ManifestAssembly](_.assemble(fragments))

  /** Composed recipe: `Live` is an ordinary class with an ordinary
    * constructor dependency; `ZLayer.fromFunction` lifts that constructor
    * into the dependency graph (Zionomicon ch. 17–18). Nothing magical —
    * layers are memoized, composable construction plans.
    */
  val live: ZLayer[GraphValidation, Nothing, ManifestAssembly] =
    ZLayer.fromFunction(Live(_))

  private final case class Live(validation: GraphValidation) extends ManifestAssembly:
    def assemble(
        fragments: List[GraphFragment]
    ): IO[::[PipelineValidationError], PipelineManifest] =
      val merged = GraphFragment.mergeAll(fragments)

      // Lineage the flows actually have: every NamedTable read is an edge.
      val derivedEdges = merged.flows.flatMap { flow =>
        Flow.reads(flow.relation).map(dev.sdp.core.DependencyEdge(_, flow.target))
      }.toSet

      val declared = merged.nodes.map(_.id).toSet
      val targetErrors = merged.flows
        .filterNot(f => declared.contains(f.target))
        .sortBy(f => (f.target, f.name))
        .map(f => PipelineValidationError.UnknownFlowTarget(f.name, f.target))

      validation
        .validate(merged.nodes, merged.edges ++ derivedEdges)
        .map(graph => (targetErrors, Some(graph)))
        .catchAll(graphErrors => ZIO.succeed((targetErrors ++ graphErrors.toList, None)))
        .flatMap {
          case (Nil, Some(graph)) =>
            // Structure is sound — run gradual schema propagation in
            // topological order (schemas declared at leaves flow downstream;
            // unknown shapes are simply unchecked).
            val checks = schemaErrors(graph, merged.flows) ++
              merged.flows.flatMap(f => dev.sdp.core.InlineDataGuard.check(f.name, f.relation))
            checks match
              case Nil          => ZIO.succeed(PipelineManifest.fromGraphAndFlows(graph, merged.flows))
              case head :: tail => ZIO.fail(::(head, tail))
          case (head :: tail, _) =>
            ZIO.fail(::(head, tail))
          case (Nil, None) =>
            ZIO.die(new IllegalStateException("validation failed with no errors — domain bug"))
        }

    /** Propagate dataset shapes through the validated graph and collect
      * column-existence errors from every flow, in topological order.
      */
    private def schemaErrors(
        graph: dev.sdp.core.PipelineGraph,
        flows: List[Flow],
    ): List[PipelineValidationError] =
      import dev.sdp.core.algebra.SchemaCheck

      val order  = graph.topologicalSort.getOrElse(Nil) // Right by construction here
      val byTarget = flows.groupBy(_.target).view.mapValues(_.map(f => (f.name, f.relation))).toMap
      val (errors, _) = SchemaCheck.propagate(order, byTarget)
      errors.map { case (flowName, SchemaCheck.SchemaError.UnknownColumn(col, available)) =>
        PipelineValidationError.UnknownColumn(flowName, col, available)
      }
