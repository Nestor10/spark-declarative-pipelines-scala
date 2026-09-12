package dev.sdp.core

/** The contribution of a single declaration site (one `table`/`streamingTable`/
  * `view` value) to the overall pipeline graph.
  *
  * Fragments form a commutative monoid under [[GraphFragment.merge]]:
  * `merge` is associative and commutative, with [[GraphFragment.empty]] as
  * identity. That algebra is why assembly order can never matter — the
  * fragments listed in a `Pipeline(...)`, in any order, merge to the same
  * graph. Duplicate ids are deliberately *representable* here (two modules can
  * both declare "orders"); they're caught by validation after the merge, where
  * the conflict is actually observable.
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

  /** The fragment-format version, carried as a header line on every rendered
    * fragment (P3.1).
    *
    * WHY a marker at all: this string crosses the plugin's classloader
    * boundary, and the two sides are two DIFFERENT copies of sdp — the plugin
    * decodes with its own, the string is produced by the one on the user's
    * runtime classpath. They ship in lockstep (the plugin injects its matching
    * library version), so a mismatch is always a misconfiguration — but
    * without a marker it surfaced as "unrecognized line (3 fields)", which
    * names the symptom and hides the cause. With it, the decoder can say which
    * side is ahead.
    *
    * Bump ONLY when bytes that someone already wrote change meaning (D14).
    * Adding an optional, trailing, omitted-when-empty field is not a bump.
    */
  val FormatVersion: Int = 1

  private val HeaderPrefix = "sdp-fragment/"
  private def header: String = s"$HeaderPrefix$FormatVersion"

  /** Canonical rendering in the shared line dialect (same as the manifest
    * body): a `sdp-fragment/N` header, then nodes sorted by id, edges sorted
    * by (from, to), flows sorted by (target, name). This is the form a
    * fragment takes when it crosses the plugin's classloader boundary (see
    * [[PipelineExport]]).
    */
  def render(fragment: GraphFragment): String =
    val nodeLines = fragment.nodes.sortBy(_.id).map(LineCodec.renderNode)
    val edgeLines = fragment.edges.toList.sortBy(e => (e.from, e.to)).map(LineCodec.renderEdge)
    val flowLines = fragment.flows.sortBy(f => (f.target, f.name)).map(LineCodec.renderFlow)
    (header :: nodeLines ::: edgeLines ::: flowLines).mkString("\n")

  /** Inverse of [[render]]. Malformed lines surface as `Left` carrying the
    * line-level diagnostic (which field, which inner parse failed) — callers
    * decide whether that's an error (hand-written input) or a defect (a
    * generated constant). Total: no input throws.
    *
    * Three header branches, deliberately:
    *   - `sdp-fragment/1` — this version: parse the body as usual;
    *   - any other `sdp-fragment/N` — a dedicated error naming BOTH versions,
    *     because the only cause is a plugin/library pair out of lockstep and
    *     the fix is to align them, not to debug the line;
    *   - no header at all — a pre-marker fragment (sdp <= 0.2.1) parsed
    *     exactly as before. BACK-COMPAT WINDOW: remove at 0.4, after which an
    *     unmarked fragment is itself the skew signal.
    */
  def parse(text: String): Either[String, GraphFragment] =
    text.linesIterator.filter(_.nonEmpty).toList match
      case head :: body if head.startsWith(HeaderPrefix) => checkVersion(head).flatMap(_ => parseBody(body))
      case unmarked                                      => parseBody(unmarked)

  private def checkVersion(headerLine: String): Either[String, Unit] =
    headerLine.drop(HeaderPrefix.length).toIntOption match
      case Some(FormatVersion) => Right(())
      case Some(newer) if newer > FormatVersion =>
        Left(
          s"fragment encoded by a newer sdp ($headerLine); this sdp understands $header — " +
            "align versions (the sbt plugin and the sdp library ship in lockstep; " +
            "check the plugin version in project/plugins.sbt against the sdp on the project classpath)"
        )
      case _ =>
        Left(
          s"unknown fragment format '$headerLine'; this sdp understands $header — " +
            "align versions (the sbt plugin and the sdp library ship in lockstep)"
        )

  private def parseBody(lines: List[String]): Either[String, GraphFragment] =
    lines.foldLeft[Either[String, GraphFragment]](Right(empty)) {
      case (acc @ Left(_), _) => acc
      case (Right(fragment), line) =>
        LineCodec.parseLine(line).map {
          case LineCodec.ParsedLine.NodeLine(node) =>
            fragment.copy(nodes = fragment.nodes :+ node)
          case LineCodec.ParsedLine.EdgeLine(edge) =>
            fragment.copy(edges = fragment.edges + edge)
          case LineCodec.ParsedLine.FlowLine(flow) =>
            fragment.copy(flows = fragment.flows :+ flow)
        }
    }
