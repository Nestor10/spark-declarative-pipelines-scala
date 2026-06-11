package dev.sdp.core

/** The canonical build artifact: a *validated* pipeline graph in a stable,
  * line-oriented textual form.
  *
  * Every later layer consumes this — the sbt cached task hashes it, the
  * protobuf encoder reads it — so the rendering must be **canonical**:
  *   - nodes sorted by id, edges sorted by (from, to), flows sorted by
  *     (target, name);
  *   - fields percent-encoded so ids/SQL/relation trees can contain any
  *     character;
  *   - no timestamps, no host paths, nothing environment-dependent.
  *
  * Two graphs with the same content always render to identical bytes,
  * regardless of how their Maps/Sets were built. JSON interop is explicitly
  * *not* a goal here; wire interop arrives with the protobuf encoder.
  *
  * Format history: v1 = nodes + edges (shape only); v2 adds `flow|` lines
  * carrying canonical relation trees; v3 generalizes `flow|` lines to carry
  * [[FlowDetails]] (AUTO CDC) and a `once` flag (a fifth field). The parser
  * stays trilingual — a v1 manifest parses as a v2 with no flows; a v2 parses
  * as a v3 with only `WriteRelation`/`once = false` flows. The writer chooses
  * the **lowest** version that can express the graph: a graph using no v3
  * construct still renders as v2, byte-identical to before (cache stability +
  * old-reader compatibility).
  */
final case class PipelineManifest(
    formatVersion: Int,
    nodes: List[PipelineNode],   // sorted by id — maintained by construction
    edges: List[DependencyEdge], // sorted by (from, to) — maintained by construction
    flows: List[Flow] = Nil,     // sorted by (target, name) — maintained by construction
):

  /** Canonical, byte-stable rendering. */
  def render: String =
    val header    = s"${PipelineManifest.Header}/${formatVersion}"
    val nodeLines = nodes.map(LineCodec.renderNode)
    val edgeLines = edges.map(LineCodec.renderEdge)
    val flowLines = flows.map(LineCodec.renderFlow)
    (header :: nodeLines ::: edgeLines ::: flowLines).mkString("", "\n", "\n")

  /** Rehydrate the (shape) graph this manifest describes. */
  def toGraph: PipelineGraph =
    PipelineGraph(nodes.map(n => n.id -> n).toMap, edges.toSet)

object PipelineManifest:

  /** The highest format version this build can emit. The writer still picks
    * the *lowest* version that expresses a given graph (see [[versionFor]]). */
  val CurrentVersion: Int = 3
  private val SupportedVersions = Set(1, 2, 3)
  private val Header            = "sdp-manifest"

  /** The lowest format version that can express `flows`: v2 unless some flow
    * needs a v3 construct (AUTO CDC or `once = true`). A graph with no v3
    * construct renders byte-identically to the pre-v3 manifest. */
  def versionFor(flows: List[Flow]): Int =
    if flows.forall(LineCodec.isV2Flow) then 2 else 3

  /** Canonicalize a validated graph (no explicit flows). */
  def fromGraph(graph: PipelineGraph): PipelineManifest =
    fromGraphAndFlows(graph, Nil)

  /** Canonicalize a validated graph plus its authored flows. */
  def fromGraphAndFlows(graph: PipelineGraph, flows: List[Flow]): PipelineManifest =
    val sortedFlows = flows.sortBy(f => (f.target, f.name))
    PipelineManifest(
      versionFor(sortedFlows),
      graph.nodes.values.toList.sortBy(_.id),
      graph.edges.toList.sortBy(e => (e.from, e.to)),
      sortedFlows,
    )

  enum ParseError:
    case MissingHeader
    case UnsupportedVersion(found: String)
    case MalformedLine(lineNumber: Int, line: String)

  /** Parse a rendered manifest. Inverse of [[PipelineManifest.render]]:
    * `parse(m.render) == Right(m)` for any canonically constructed `m`.
    * Input entries are re-sorted, so even a hand-edited file parses to
    * canonical form. v1 input yields `formatVersion = 1` and no flows.
    */
  def parse(text: String): Either[ParseError, PipelineManifest] =
    text.linesIterator.toList match
      case Nil => Left(ParseError.MissingHeader)
      case header :: body =>
        for
          version <- parseHeader(header)
          entries <- parseBody(body)
        yield
          val (nodes, edges, flows) = entries
          PipelineManifest(
            version,
            nodes.sortBy(_.id),
            edges.sortBy(e => (e.from, e.to)),
            flows.sortBy(f => (f.target, f.name)),
          )

  private def parseHeader(line: String): Either[ParseError, Int] =
    line.split('/') match
      case Array(Header, v) =>
        v.toIntOption.filter(SupportedVersions.contains).toRight(ParseError.UnsupportedVersion(v))
      case _ => Left(ParseError.MissingHeader)

  private def parseBody(
      lines: List[String]
  ): Either[ParseError, (List[PipelineNode], List[DependencyEdge], List[Flow])] =
    // Right-biased accumulation with early exit on the first malformed line:
    // a malformed manifest is corrupt input, not author error — fail fast.
    lines.zipWithIndex
      .filterNot((line, _) => line.isEmpty)
      .foldLeft[Either[ParseError, (List[PipelineNode], List[DependencyEdge], List[Flow])]](
        Right((Nil, Nil, Nil))
      ) {
        case (acc @ Left(_), _) => acc
        case (Right((nodes, edges, flows)), (line, idx)) =>
          LineCodec.parseLine(line) match
            case Some(LineCodec.ParsedLine.NodeLine(node)) => Right((node :: nodes, edges, flows))
            case Some(LineCodec.ParsedLine.EdgeLine(edge)) => Right((nodes, edge :: edges, flows))
            case Some(LineCodec.ParsedLine.FlowLine(flow)) => Right((nodes, edges, flow :: flows))
            case None => Left(ParseError.MalformedLine(idx + 2, line)) // +2: 1-based, after header
      }
      .map((ns, es, fs) => (ns.reverse, es.reverse, fs.reverse))
