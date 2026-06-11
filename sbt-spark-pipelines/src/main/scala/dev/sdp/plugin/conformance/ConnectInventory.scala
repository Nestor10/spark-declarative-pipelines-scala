package dev.sdp.plugin.conformance

import scala.jdk.CollectionConverters.*

import com.google.protobuf.Descriptors.{Descriptor, EnumDescriptor, FieldDescriptor, FileDescriptor}
import org.apache.spark.connect.proto as sc

/** The machine-readable spec, made inspectable.
  *
  * Walks the protobuf `FileDescriptor`s of the *pinned* Spark Connect
  * artifact (the same generated classes the encoder uses) and renders every
  * `spark.connect` message, field, and enum as a deterministic text
  * inventory. The inventory is the substrate for:
  *
  *   - the **drift gate**: the rendered inventory is compared against a
  *     checked-in golden snapshot, so upgrading the Spark artifact surfaces
  *     every wire-surface change as a field-level diff that must be
  *     consciously accepted;
  *   - the **coverage report**: capability ids (`<message>#<field>`) are the
  *     keys our support claims are verified against.
  */
object ConnectInventory:

  /** Capability id for a field: `spark.connect.Relation#read`. */
  def capabilityId(message: Descriptor, field: FieldDescriptor): String =
    s"${message.getFullName}#${field.getName}"

  /** All `spark.connect` file descriptors reachable from the messages this
    * project touches, transitively, deduplicated, sorted by file name.
    */
  def files: List[FileDescriptor] =
    val roots = List(
      sc.PipelineCommand.getDescriptor.getFile,
      sc.Relation.getDescriptor.getFile,
      sc.Expression.getDescriptor.getFile,
      sc.DataType.getDescriptor.getFile,
    )

    def closure(seen: Map[String, FileDescriptor], todo: List[FileDescriptor]): Map[String, FileDescriptor] =
      todo match
        case Nil => seen
        case f :: rest =>
          if seen.contains(f.getName) then closure(seen, rest)
          else closure(seen + (f.getName -> f), f.getDependencies.asScala.toList ::: rest)

    closure(Map.empty, roots).values.toList
      .filter(_.getPackage == "spark.connect")
      .sortBy(_.getName)

  /** Every field capability id in the spark.connect surface. */
  def allCapabilityIds: Set[String] =
    val ids = Set.newBuilder[String]
    def walkMessage(m: Descriptor): Unit =
      m.getFields.asScala.foreach(f => ids += capabilityId(m, f))
      m.getNestedTypes.asScala.foreach(walkMessage)
    files.foreach(_.getMessageTypes.asScala.foreach(walkMessage))
    ids.result()

  /** Deterministic textual inventory of the whole surface. */
  def render: String =
    val sb = new StringBuilder

    def fieldLine(f: FieldDescriptor): String =
      val tpe = f.getType match
        case FieldDescriptor.Type.MESSAGE => s"message:${f.getMessageType.getFullName}"
        case FieldDescriptor.Type.ENUM    => s"enum:${f.getEnumType.getFullName}"
        case other                        => other.name.toLowerCase
      // toProto exposes proto3-optional reliably; hasOptionalKeyword is protobuf-internal.
      val label = if f.isRepeated then "repeated " else if f.toProto.getProto3Optional then "optional " else ""
      val oneof = Option(f.getContainingOneof).map(o => s" oneof=${o.getName}").getOrElse("")
      s"    field ${f.getNumber} ${f.getName} $label$tpe$oneof"

    def walkEnum(indent: String, e: EnumDescriptor): Unit =
      sb.append(s"$indent  enum ${e.getFullName}\n")
      e.getValues.asScala.sortBy(_.getNumber).foreach { v =>
        sb.append(s"$indent    value ${v.getNumber} ${v.getName}\n")
      }

    def walkMessage(m: Descriptor): Unit =
      sb.append(s"  msg ${m.getFullName}\n")
      m.getFields.asScala.sortBy(_.getNumber).foreach(f => sb.append(fieldLine(f)).append('\n'))
      m.getEnumTypes.asScala.sortBy(_.getFullName).foreach(walkEnum("  ", _))
      m.getNestedTypes.asScala.sortBy(_.getFullName).foreach(walkMessage)

    files.foreach { file =>
      sb.append(s"file ${file.getName}\n")
      file.getMessageTypes.asScala.sortBy(_.getFullName).foreach(walkMessage)
      file.getEnumTypes.asScala.sortBy(_.getFullName).foreach(walkEnum("", _))
    }
    sb.result()
