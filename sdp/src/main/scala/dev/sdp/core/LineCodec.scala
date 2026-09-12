package dev.sdp.core

import java.net.{URLDecoder, URLEncoder}
import java.nio.charset.StandardCharsets.UTF_8

/** Line-level canonical codec shared by [[PipelineManifest]] (whole-graph
  * artifact) and [[GraphFragment]] (the per-declaration contribution).
  *
  * One serialization for both keeps the fragment strings that cross the
  * plugin's classloader boundary and the final manifest in the same dialect:
  * fields percent-encoded, `|`-separated, one node or edge per line.
  */
private[core] object LineCodec:

  def renderNode(node: PipelineNode): String = node match
    case PipelineNode.Table(id, format)          => s"node|${enc(id)}|table|${enc(format)}"
    case PipelineNode.StreamingTable(id, format) => s"node|${enc(id)}|streaming-table|${enc(format)}"
    case PipelineNode.MaterializedView(id, sql)  => s"node|${enc(id)}|materialized-view|${enc(sql)}"
    case PipelineNode.TemporaryView(id, sql)     => s"node|${enc(id)}|temporary-view|${enc(sql)}"
    case PipelineNode.ExternalTable(id)          => s"node|${enc(id)}|external|"

  def renderEdge(edge: DependencyEdge): String =
    s"edge|${enc(edge.from)}|${enc(edge.to)}"

  /** A flow whose details are a plain [[FlowDetails.WriteRelation]] and whose
    * `once` is `false` — i.e. expressible in manifest format v2. Such flows
    * render byte-identically to the v2 four-field `flow|` line, so old
    * manifests parse unchanged and new graphs that use no v3 construct keep
    * writing v2. */
  def isV2Flow(flow: Flow): Boolean =
    !flow.once && (flow.details match
      case _: FlowDetails.WriteRelation => true
      case _: FlowDetails.AutoCdc       => false)

  def renderFlow(flow: Flow): String =
    flow.details match
      case FlowDetails.WriteRelation(rel) if !flow.once =>
        // v2 line — byte-identical to every manifest written before v3.
        s"flow|${enc(flow.name)}|${enc(flow.target)}|${enc(algebra.RelCodec.render(rel))}"
      case _ =>
        // v3 line — fifth field carries `once`; fourth is a FlowDetails render.
        val once = if flow.once then "t" else "f"
        s"flow|${enc(flow.name)}|${enc(flow.target)}|${enc(FlowCodec.renderDetails(flow.details))}|$once"

  enum ParsedLine:
    case NodeLine(node: PipelineNode)
    case EdgeLine(edge: DependencyEdge)
    case FlowLine(flow: Flow)

  /** One parsed line — or a `Left` naming what went wrong.
    *
    * TOTAL: no input throws. Field decoding goes through [[decode]], and the
    * inner `Rel`/`FlowDetails` diagnostic is PROPAGATED rather than collapsed
    * to "malformed" — at the classloader/version-skew boundary the inner
    * message ("unknown rel tag 'pivot'") is the whole story.
    *
    * `split` with limit -1 keeps trailing empty fields: a node whose last
    * field is the empty string (e.g. an MV with an authored flow instead of
    * node-level SQL) must still parse as four fields.
    */
  def parseLine(line: String): Either[String, ParsedLine] =
    line.split("\\|", -1) match
      case Array("node", id, "table", format) =>
        for i <- decode(id); f <- decode(format)
        yield ParsedLine.NodeLine(PipelineNode.Table(i, f))
      case Array("node", id, "streaming-table", format) =>
        for i <- decode(id); f <- decode(format)
        yield ParsedLine.NodeLine(PipelineNode.StreamingTable(i, f))
      case Array("node", id, "materialized-view", sql) =>
        for i <- decode(id); s <- decode(sql)
        yield ParsedLine.NodeLine(PipelineNode.MaterializedView(i, s))
      case Array("node", id, "temporary-view", sql) =>
        for i <- decode(id); s <- decode(sql)
        yield ParsedLine.NodeLine(PipelineNode.TemporaryView(i, s))
      case Array("node", id, "external", _) =>
        decode(id).map(i => ParsedLine.NodeLine(PipelineNode.ExternalTable(i)))
      case Array("edge", from, to) =>
        for f <- decode(from); t <- decode(to)
        yield ParsedLine.EdgeLine(DependencyEdge(f, t))
      case Array("flow", name, target, rel) =>
        // v2 four-field flow line: bare Rel render, once = false.
        for
          n        <- decode(name)
          t        <- decode(target)
          r        <- decode(rel)
          relation <- algebra.RelCodec.parse(r).left.map(e => s"flow '$n': $e")
        yield ParsedLine.FlowLine(Flow(n, t, relation))
      case Array("flow", name, target, details, once) =>
        // v3 five-field flow line: FlowDetails render + once flag.
        for
          n <- decode(name)
          t <- decode(target)
          d <- decode(details)
          // cheap field first, so its diagnostic is not masked by the details parse
          o <- once match
            case "t"   => Right(true)
            case "f"   => Right(false)
            case other => Left(s"flow '$n': once flag must be 't' or 'f', got '$other'")
          details0 <- FlowCodec.parseDetails(d).left.map(e => s"flow '$n': $e")
        yield ParsedLine.FlowLine(Flow(n, t, details0, o))
      case fields =>
        Left(s"unrecognized line (${fields.length} fields): '$line'")

  private[core] def enc(s: String): String = URLEncoder.encode(s, UTF_8)

  /** TOTAL percent-decode — the primitive every parsing path must use.
    *
    * `URLDecoder.decode` throws `IllegalArgumentException` on a malformed
    * escape (`%zz`, a truncated `%a`). That is an EXPECTED failure exactly
    * where it matters most: the fragment string is the cross-classloader /
    * cross-version boundary, so a plugin and a library out of lockstep (or a
    * hand-edited manifest) can hand us input no amount of local correctness
    * prevents. The codec contract is total parsing with the offending input
    * named, so the failure is a `Left`, never a throw. */
  private[core] def decode(s: String): Either[String, String] =
    try Right(URLDecoder.decode(s, UTF_8))
    catch
      case e: IllegalArgumentException =>
        Left(s"malformed percent-encoding in '$s' (${e.getMessage})")

  /** Decode an atom ALREADY PROVEN decodable by [[decode]].
    *
    * Only for use after a single up-front validation pass, so the recursive
    * descent need not thread an `Either` through every positional field:
    * `RelCodec.Sexp.read` rejects any atom that cannot decode before the
    * descent runs, and `parseLine` below decodes its own fields with [[decode]].
    * It is NOT a parsing entry point — never call it on unvalidated input. */
  private[core] def dec(s: String): String = URLDecoder.decode(s, UTF_8)
