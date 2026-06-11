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

  def renderFlow(flow: Flow): String =
    s"flow|${enc(flow.name)}|${enc(flow.target)}|${enc(algebra.RelCodec.render(flow.relation))}"

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
        algebra.RelCodec.parse(dec(rel)).toOption.map { relation =>
          ParsedLine.FlowLine(Flow(dec(name), dec(target), relation))
        }
      case _ => None

  private[core] def enc(s: String): String = URLEncoder.encode(s, UTF_8)
  private[core] def dec(s: String): String = URLDecoder.decode(s, UTF_8)
