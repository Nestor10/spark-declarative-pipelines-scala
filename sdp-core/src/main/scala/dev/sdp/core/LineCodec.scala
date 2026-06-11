package dev.sdp.core

import java.net.{URLDecoder, URLEncoder}
import java.nio.charset.StandardCharsets.UTF_8

/** Line-level canonical codec shared by [[PipelineManifest]] (whole-graph
  * artifact) and [[GraphFragment]] (per-call-site embedded constant).
  *
  * One serialization for both keeps the TASTy-embedded fragments and the
  * final manifest in the same dialect: fields percent-encoded, `|`-separated,
  * one node or edge per line.
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

  /** One parsed line — or None for anything malformed.
    *
    * `split` with limit -1 keeps trailing empty fields: a node whose last
    * field is the empty string (e.g. an MV with an authored flow instead of
    * node-level SQL) must still parse as four fields.
    */
  def parseLine(line: String): Option[ParsedLine] =
    line.split("\\|", -1) match
      case Array("node", id, "table", format) =>
        Some(ParsedLine.NodeLine(PipelineNode.Table(dec(id), dec(format))))
      case Array("node", id, "streaming-table", format) =>
        Some(ParsedLine.NodeLine(PipelineNode.StreamingTable(dec(id), dec(format))))
      case Array("node", id, "materialized-view", sql) =>
        Some(ParsedLine.NodeLine(PipelineNode.MaterializedView(dec(id), dec(sql))))
      case Array("node", id, "temporary-view", sql) =>
        Some(ParsedLine.NodeLine(PipelineNode.TemporaryView(dec(id), dec(sql))))
      case Array("node", id, "external", _) =>
        Some(ParsedLine.NodeLine(PipelineNode.ExternalTable(dec(id))))
      case Array("edge", from, to) =>
        Some(ParsedLine.EdgeLine(DependencyEdge(dec(from), dec(to))))
      case Array("flow", name, target, rel) =>
        // v2 four-field flow line: bare Rel render, once = false.
        algebra.RelCodec.parse(dec(rel)).toOption.map { relation =>
          ParsedLine.FlowLine(Flow(dec(name), dec(target), relation))
        }
      case Array("flow", name, target, details, once) =>
        // v3 five-field flow line: FlowDetails render + once flag.
        for
          d <- FlowCodec.parseDetails(dec(details)).toOption
          o <- once match
            case "t" => Some(true); case "f" => Some(false); case _ => None
        yield ParsedLine.FlowLine(Flow(dec(name), dec(target), d, o))
      case _ => None

  private[core] def enc(s: String): String = URLEncoder.encode(s, UTF_8)
  private[core] def dec(s: String): String = URLDecoder.decode(s, UTF_8)
