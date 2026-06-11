package dev.sdp.app

import dev.sdp.core.{DependencyEdge, PipelineGraph, PipelineNode, PipelineValidationError}
import zio.*

/** Application Services ring: validates raw graph fragments into a
  * structurally sound [[PipelineGraph]].
  *
  * The error channel is `::[PipelineValidationError]` — the cons type — so a
  * failure *provably* carries at least one error. The domain's smart
  * constructor established that invariant; the service merely lifts it into
  * ZIO without weakening it (no `Throwable` widening, no `.orDie`).
  *
  * Why a service at all for pure logic? The capability, not the computation,
  * is the unit of architecture: the sbt task and future cache-aware or
  * remote-schema-aware validators swap in via `ZLayer` without touching call
  * sites (Zionomicon ch. 17).
  */
trait GraphValidation:
  def validate(
      fragments: List[PipelineNode],
      edges: Set[DependencyEdge],
  ): IO[::[PipelineValidationError], PipelineGraph]

object GraphValidation:

  /** Accessor: lets callers write `GraphValidation.validate(...)` against the
    * environment instead of threading the service by hand.
    */
  def validate(
      fragments: List[PipelineNode],
      edges: Set[DependencyEdge],
  ): ZIO[GraphValidation, ::[PipelineValidationError], PipelineGraph] =
    ZIO.serviceWithZIO[GraphValidation](_.validate(fragments, edges))

  val live: ULayer[GraphValidation] = ZLayer.succeed(Live())

  private final class Live extends GraphValidation:
    def validate(
        fragments: List[PipelineNode],
        edges: Set[DependencyEdge],
    ): IO[::[PipelineValidationError], PipelineGraph] =
      ZIO.fromEither(PipelineGraph.fromFragments(fragments, edges))
