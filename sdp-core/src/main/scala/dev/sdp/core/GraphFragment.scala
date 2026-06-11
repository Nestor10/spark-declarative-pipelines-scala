package dev.sdp.core

/** The contribution of a single compilation unit (one macro expansion, one
  * source file) to the overall pipeline graph.
  *
  * Fragments form a commutative monoid under [[GraphFragment.merge]]:
  * `merge` is associative and commutative, with [[GraphFragment.empty]] as
  * identity. That algebra is why assembly order can never matter — N modules
  * compiled in any order contribute the same merged graph. Duplicate ids are
  * deliberately *representable* here (two modules can both declare "orders");
  * they're caught by validation after the merge, where the conflict is
  * actually observable.
  */
final case class GraphFragment(
    nodes: List[PipelineNode],
    edges: Set[DependencyEdge],
    flows: List[Flow] = Nil,
):
  def merge(other: GraphFragment): GraphFragment =
    GraphFragment(nodes ++ other.nodes, edges ++ other.edges, flows ++ other.flows)

object GraphFragment:
  val empty: GraphFragment = GraphFragment(Nil, Set.empty, Nil)

  def mergeAll(fragments: List[GraphFragment]): GraphFragment =
    fragments.foldLeft(empty)(_.merge(_))

  /** Canonical rendering in the shared line dialect (same as the manifest
    * body): nodes sorted by id, edges sorted by (from, to), flows sorted by
    * (target, name). This is the form the DSL macros embed as a string
    * constant at each call site.
    */
  def render(fragment: GraphFragment): String =
    val nodeLines = fragment.nodes.sortBy(_.id).map(LineCodec.renderNode)
    val edgeLines = fragment.edges.toList.sortBy(e => (e.from, e.to)).map(LineCodec.renderEdge)
    val flowLines = fragment.flows.sortBy(f => (f.target, f.name)).map(LineCodec.renderFlow)
    (nodeLines ::: edgeLines ::: flowLines).mkString("\n")

  /** Inverse of [[render]]. Malformed lines surface as `Left` with the
    * offending content — callers decide whether that's an error (hand-written
    * input) or a defect (macro-generated constant).
    */
  def parse(text: String): Either[String, GraphFragment] =
    val lines = text.linesIterator.filter(_.nonEmpty).toList
    lines.foldLeft[Either[String, GraphFragment]](Right(empty)) {
      case (acc @ Left(_), _) => acc
      case (Right(fragment), line) =>
        LineCodec.parseLine(line) match
          case Some(LineCodec.ParsedLine.NodeLine(node)) =>
            Right(fragment.copy(nodes = fragment.nodes :+ node))
          case Some(LineCodec.ParsedLine.EdgeLine(edge)) =>
            Right(fragment.copy(edges = fragment.edges + edge))
          case Some(LineCodec.ParsedLine.FlowLine(flow)) =>
            Right(fragment.copy(flows = fragment.flows :+ flow))
          case None => Left(line)
    }

  /** Trusted parse for macro-generated constants only. A malformed constant
    * is a bug in the DSL macros, not author error — so it dies as a defect
    * rather than occupying an error channel every caller would have to
    * thread (Zionomicon ch. 3: defects are for the impossible).
    */
  def parseTrusted(text: String): GraphFragment =
    parse(text).fold(
      bad => throw new IllegalStateException(
        s"Malformed macro-embedded fragment line: '$bad'. This is a bug in sdp-runtime-dsl."
      ),
      identity,
    )
