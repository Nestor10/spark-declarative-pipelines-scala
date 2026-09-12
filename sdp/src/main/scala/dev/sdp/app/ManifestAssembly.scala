package dev.sdp.app

import dev.sdp.core.{Flow, GraphFragment, PipelineManifest, PipelineValidationError}
import zio.*

/** Assembles per-compilation-unit fragments into the canonical manifest.
  *
  * This is the exact entry point the sbt task calls: the evaluated fragments
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

      // Lineage the flows actually have: every read becomes an edge. For a
      // WriteRelation flow that is each NamedTable read; for an AUTO CDC flow
      // it is exactly the `source` (so a missing source falls out of the
      // existing dangling-edge detection — no separate rule needed).
      val derivedEdges = merged.flows.flatMap { flow =>
        Flow.reads(flow).map(dev.sdp.core.DependencyEdge(_, flow.target))
      }.toSet

      val declared = merged.nodes.map(_.id).toSet
      val targetErrors = merged.flows
        .filterNot(f => declared.contains(f.target))
        .sortBy(f => (f.target, f.name))
        .map(f => PipelineValidationError.UnknownFlowTarget(f.name, f.target))

      // AUTO-CDC-specific structural rules (target shape, key presence).
      val cdcErrors = cdcStructuralErrors(merged.flows, merged.nodes)

      val preErrors = targetErrors ++ cdcErrors

      validation
        .validate(merged.nodes, merged.edges ++ derivedEdges)
        .map(graph => (preErrors, Some(graph)))
        .catchAll(graphErrors => ZIO.succeed((preErrors ++ graphErrors.toList, None)))
        .flatMap {
          case (Nil, Some(graph)) =>
            // Structure is sound — run gradual schema propagation in
            // topological order (schemas declared at leaves flow downstream;
            // unknown shapes are simply unchecked). Only WriteRelation flows
            // carry relations to check / inline data to guard; AUTO CDC flows
            // contribute neither (their target shape is gradual-Unknown).
            val relFlows = merged.flows.filter {
              _.details match
                case _: dev.sdp.core.FlowDetails.WriteRelation => true
                case _: dev.sdp.core.FlowDetails.AutoCdc       => false
            }
            val checks = schemaErrors(graph, relFlows) ++
              relFlows.flatMap(f => dev.sdp.core.InlineDataGuard.check(f.name, f.relation))
            checks match
              case Nil          => ZIO.succeed(PipelineManifest.fromGraphAndFlows(graph, merged.flows))
              case head :: tail => ZIO.fail(::(head, tail))
          case (head :: tail, _) =>
            ZIO.fail(::(head, tail))
          case (Nil, None) =>
            ZIO.die(new IllegalStateException("validation failed with no errors — domain bug"))
        }

    /** AUTO CDC structural rules, accumulated (not short-circuiting):
      *   - the target must be a declared **streaming TABLE** dataset (the CDC
      *     flow MERGEs into it; the server requires a streaming table shell);
      *   - `keys` must be non-empty (row identity is mandatory);
      *   - the SCD2 history-tracking lists are mutually exclusive, and mean
      *     nothing outside SCD2 — both refusals mirror the server's own
      *     (`AUTOCDC_BOTH_TRACK_HISTORY_COLUMN_LIST_AND_EXCEPT_COLUMN_LIST`,
      *     `AUTOCDC_TRACK_HISTORY_REQUIRES_SCD2`), caught offline so the author
      *     never spends a round trip on them.
      * The missing-`source` case is intentionally *not* here — it falls out of
      * the generic dangling-read detection via the derived `source -> target`
      * edge. */
    private def cdcStructuralErrors(
        flows: List[Flow],
        nodes: List[dev.sdp.core.PipelineNode],
    ): List[PipelineValidationError] =
      import dev.sdp.core.{FlowDetails, PipelineNode}
      val byId = nodes.map(n => n.id -> n).toMap
      flows
        .sortBy(f => (f.target, f.name))
        .collect { case f @ Flow(_, _, cdc: FlowDetails.AutoCdc, _) => (f, cdc) }
        .flatMap { (f, cdc) =>
          val targetErr = byId.get(f.target) match
            case Some(_: PipelineNode.StreamingTable) => Nil
            case Some(_) =>
              List(PipelineValidationError.AutoCdcTargetNotStreamingTable(f.name, f.target))
            case None =>
              // unknown target is already reported as UnknownFlowTarget; don't
              // double-report it as a type error.
              Nil
          val keyErr =
            if cdc.keys.isEmpty then List(PipelineValidationError.AutoCdcKeysEmpty(f.name)) else Nil
          // An EMPTY list is "track everything eligible" (exactly the wire's
          // meaning of an unset repeated field), so both rules gate on
          // non-emptiness, never on "was it specified".
          val tracked = cdc.trackHistoryColumnList.nonEmpty
          val trackedExcept = cdc.trackHistoryExceptColumnList.nonEmpty
          val bothErr =
            if tracked && trackedExcept then
              List(PipelineValidationError.AutoCdcBothTrackHistoryLists(f.name))
            else Nil
          val scdErr = cdc.scdType match
            case dev.sdp.core.ScdType.Scd2 => Nil
            case dev.sdp.core.ScdType.Scd1 =>
              if tracked || trackedExcept then
                List(PipelineValidationError.AutoCdcTrackHistoryRequiresScd2(f.name))
              else Nil
          targetErr ++ keyErr ++ bothErr ++ scdErr
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
