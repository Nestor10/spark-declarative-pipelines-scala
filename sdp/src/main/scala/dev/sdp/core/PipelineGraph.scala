package dev.sdp.core

/** Pure Scala 3 representation of a Spark Declarative Pipelines dataflow graph.
  *
  * This is the innermost ring of the Onion: zero dependencies, total functions,
  * deterministic. All structural validation lives here so the same logic runs
  * both inside the macro (compile time) and inside the ZIO runtime (sbt task time).
  *
  * Design notes:
  *   - `nodes` is keyed by id, so a constructed graph *cannot* contain duplicate
  *     ids — duplicates are caught earlier, in [[PipelineGraph.fromFragments]],
  *     which receives the raw declaration list the macro emits.
  *   - Validation accumulates: every check contributes zero or more errors and
  *     the results concatenate. Authors see all structural problems in one
  *     compile, not one per compile.
  *   - All traversals iterate in sorted order. The output of these functions
  *     feeds sbt 2.0 cached tasks, and cache keys must not depend on
  *     `Map`/`Set` iteration order.
  */
final case class PipelineGraph(
    nodes: Map[String, PipelineNode],
    edges: Set[DependencyEdge],
):

  /** All structural errors in this graph. Empty means valid.
    *
    * Accumulating (`List` as a monoid) rather than short-circuiting: dangling
    * edges and cycles are reported together.
    */
  def validate: List[PipelineValidationError] =
    danglingEdges.toList ++ cycle.map(PipelineValidationError.CycleDetected(_)).toList

  /** Execution order, or the cycle that prevents one.
    *
    * Deterministic: node ids and neighbor lists are walked in sorted order, so
    * equal graphs always yield the same ordering regardless of how the
    * underlying `Map`/`Set` happen to hash.
    */
  def topologicalSort: Either[PipelineValidationError.CycleDetected, List[String]] =
    cycle match
      case Some(path) => Left(PipelineValidationError.CycleDetected(path))
      case None =>
        // DFS post-order, reversed. Total: unknown edge targets resolve to Nil.
        def visit(node: String, seen: Set[String], order: List[String]): (Set[String], List[String]) =
          if seen(node) then (seen, order)
          else
            val (seen2, order2) = adjacency.getOrElse(node, Nil).foldLeft((seen + node, order)) {
              case ((s, o), next) => visit(next, s, o)
            }
            (seen2, node :: order2)

        val sorted = nodes.keys.toList.sorted
          .foldLeft((Set.empty[String], List.empty[String])) { case ((s, o), n) => visit(n, s, o) }
          ._2
        // Totality guard: dangling edges would otherwise inject phantom ids.
        Right(sorted.filter(nodes.contains))

  /** Edge endpoints that reference ids not present in `nodes`, sorted. */
  private def danglingEdges: Option[PipelineValidationError.DanglingEdges] =
    val unknown = edges.toList
      .flatMap(e => List(e.from, e.to))
      .filterNot(nodes.contains)
      .distinct
      .sorted
    Option.when(unknown.nonEmpty)(PipelineValidationError.DanglingEdges(unknown))

  /** First cycle found (in sorted traversal order), as a closed path
    * `a -> b -> ... -> a`, or None if the graph is acyclic.
    */
  private def cycle: Option[List[String]] =
    def dfs(node: String, visited: Set[String], stack: List[String]): (Set[String], Option[List[String]]) =
      if stack.contains(node) then
        // `stack` is the path back to the root, most recent first. Slice off
        // everything since we first saw `node`, then close the loop.
        (visited, Some(node :: (node :: stack.takeWhile(_ != node)).reverse))
      else if visited(node) then (visited, None)
      else
        adjacency.getOrElse(node, Nil).foldLeft((visited + node, Option.empty[List[String]])) {
          case ((vis, found @ Some(_)), _) => (vis, found)
          case ((vis, None), next)         => dfs(next, vis, node :: stack)
        }

    nodes.keys.toList.sorted.foldLeft((Set.empty[String], Option.empty[List[String]])) {
      case ((vis, found @ Some(_)), _) => (vis, found)
      case ((vis, None), n)            => dfs(n, vis, Nil)
    }._2

  /** Sorted adjacency list over known and unknown targets alike. */
  private lazy val adjacency: Map[String, List[String]] =
    val declared = nodes.keys.map(_ -> List.empty[String]).toMap
    val fromEdges = edges.groupBy(_.from).view.mapValues(_.map(_.to).toList.sorted).toMap
    declared ++ fromEdges

object PipelineGraph:

  /** Assemble a graph from raw declaration fragments — the shape the macro
    * layer emits. This is the smart constructor: duplicate ids are only
    * representable *here*, before the fragments collapse into the keyed map,
    * so this is where they're caught.
    *
    * Returns either every structural error found (duplicates first, then
    * dangling edges and cycles) or a graph that is valid by construction.
    * The error side is `::[E]` — stdlib's cons type — so "failed with zero
    * errors" is unrepresentable. Downstream (the ZIO service layer) inherits
    * a non-empty error channel for free.
    */
  def fromFragments(
      fragments: List[PipelineNode],
      edges: Set[DependencyEdge],
  ): Either[::[PipelineValidationError], PipelineGraph] =
    val duplicates = fragments
      .groupBy(_.id)
      .collect { case (id, group) if group.sizeIs > 1 => id }
      .toList
      .sorted
      .map(PipelineValidationError.DuplicateNode(_))

    val graph = PipelineGraph(fragments.map(n => n.id -> n).toMap, edges)
    duplicates ++ graph.validate match
      case head :: tail => Left(::(head, tail))
      case Nil          => Right(graph)

/** A node in the pipeline graph. Every node carries its dataset id. */
enum PipelineNode(val id: String):
  case Table(override val id: String, format: String)            extends PipelineNode(id)
  case StreamingTable(override val id: String, format: String)   extends PipelineNode(id)
  case MaterializedView(override val id: String, sql: String)    extends PipelineNode(id)
  case TemporaryView(override val id: String, sql: String)       extends PipelineNode(id)

  /** An *external* / source table the pipeline reads but does not own — it
    * already exists in the catalog. Declaring it makes reads of it resolve
    * (not dangling) and marks it a DAG source (no flow, no incoming edges),
    * but it is **never** emitted to the server as a managed dataset: the
    * analyzer resolves it from the catalog as a `usedExternalInput`. The id
    * is the (possibly qualified) catalog identifier. */
  case ExternalTable(override val id: String)                    extends PipelineNode(id)

/** A directed edge from an upstream producer to a downstream consumer. */
final case class DependencyEdge(from: String, to: String)

/** Errors the structural validator can surface. A closed ADT so the ZIO layer
  * can expose a precisely typed error channel (Zionomicon ch. 3: errors you
  * expect callers to handle belong in the type).
  */
enum PipelineValidationError:
  case CycleDetected(path: List[String])
  case DuplicateNode(id: String)
  case DanglingEdges(invalidNodeIds: List[String])
  case UnknownFlowTarget(flowName: String, target: String)
  case UnknownColumn(flowName: String, column: String, available: List[String])
  case InlineTableTooLarge(flowName: String, rows: Int, estimatedBytes: Long)

  /** AUTO CDC flow whose target dataset exists but is not a streaming TABLE
    * (the CDC MERGE requires a streaming-table shell as its target). */
  case AutoCdcTargetNotStreamingTable(flowName: String, target: String)

  /** AUTO CDC flow declared with no `keys` (row identity is mandatory). */
  case AutoCdcKeysEmpty(flowName: String)

  /** Human-readable, single-line description for build logs. */
  def describe: String = this match
    case CycleDetected(path) =>
      s"cyclic dependency: ${path.mkString(" -> ")}"
    case DuplicateNode(id) =>
      s"dataset '$id' is declared more than once"
    case DanglingEdges(ids) =>
      s"lineage references undeclared dataset(s): ${ids.mkString(", ")}"
    case UnknownFlowTarget(flowName, target) =>
      s"flow '$flowName' targets undeclared dataset '$target'"
    case UnknownColumn(flowName, column, available) =>
      s"flow '$flowName' references unknown column '$column' (available: ${available.mkString(", ")})"
    case InlineTableTooLarge(flowName, rows, estimatedBytes) =>
      s"flow '$flowName' has an inline table that is too large ($rows rows, ~$estimatedBytes bytes) — " +
        s"inline data (createDataFrame) is for small lookup/seed tables; load larger data from a source table"
    case AutoCdcTargetNotStreamingTable(flowName, target) =>
      s"AUTO CDC flow '$flowName' targets '$target', which is not a streaming table — " +
        s"create the target with createStreamingTable(\"$target\") (apply_changes MERGEs into a streaming table)"
    case AutoCdcKeysEmpty(flowName) =>
      s"AUTO CDC flow '$flowName' declares no keys — at least one key column is required to identify rows"
