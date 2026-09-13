package dev.sdp.connect

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters.*

import com.google.protobuf.TextFormat
import dev.sdp.core.PipelineManifest
import org.apache.spark.connect.proto as sc

/** **What we said**, in the only form a human can review: the full Spark
  * Connect registration sequence rendered as protobuf TEXT FORMAT.
  *
  * Why this exists. The SDP wire is a one-way mirror: the server logs nothing
  * about the plans it receives (verified absent in `SparkConnectService`) and
  * `pipelines.proto` has **no graph-readback command** — once
  * `CreateDataflowGraph` succeeds, the dataflow graph the server built is
  * unobservable by design. So when a dependency edge silently fails to appear,
  * the only two questions anyone can actually answer are "what did we send?"
  * and "how does the server *read* what we send?". This object answers the
  * first; [[PlanExplain]] answers the second.
  *
  * Two properties make the dump worth attaching to a bug report:
  *
  *   - **It is the same bytes.** Every command comes from
  *     [[PipelineProtoEncoder]] — the encoder the live registration uses. There
  *     is no second rendering path that could drift (that is the whole point:
  *     a dump that is "approximately what we send" proves nothing).
  *   - **It is deterministic.** The encoder is pure and the manifest is
  *     canonically sorted, so equal manifests dump byte-identical files. The
  *     one genuinely server-assigned value, the dataflow graph id, is rendered
  *     as [[GraphIdPlaceholder]] rather than a fresh UUID — otherwise every
  *     dump would differ from the last one for no reason anybody cares about.
  *
  * The text format round-trips: `TextFormat.merge` parses any file here back
  * into the message it came from (asserted in `WireDumpSpec`), so a dump can be
  * edited, replayed, or diffed against another client's.
  */
object WireDump:

  /** The graph id is assigned by the server in response to
    * `CreateDataflowGraph`; an offline dump has none. Rendering a placeholder
    * (rather than a fresh UUID) is what keeps two dumps of the same pipeline
    * byte-identical. */
  val GraphIdPlaceholder: String = "<dataflow-graph-id>"

  /** File extension for protobuf text format — the conventional one
    * (`.txtpb`), so editors and `protoc --decode` recognise it. */
  private val Extension = "txtpb"

  /** One command of the sequence: the file it belongs in, the message itself
    * (so a test can compare structurally), and its rendering. */
  final case class Entry(fileName: String, command: sc.PipelineCommand, prototext: String)

  /** Everything the registration sequence needs that is NOT in the manifest —
    * i.e. the connection-shaped values a target supplies. Defaults match
    * `PipelinesRegistration.register`'s so a bare dump is the bare run.
    */
  final case class Options(
      defaultCatalog: Option[String] = None,
      defaultDatabase: Option[String] = None,
      sqlConf: Map[String, String] = Map.empty,
      storage: String = "file:///tmp/sdp-dry-run",
      dry: Boolean = true,
      fullRefresh: Boolean = false,
  )

  /** The whole sequence, in send order: `CreateDataflowGraph`, every
    * `DefineOutput`/`DefineFlow` exactly as [[PipelineProtoEncoder.definitions]]
    * orders them, then `StartRun`.
    *
    * Pure. The numbering is positional so a directory listing sorts into the
    * send order, and `99-start-run` is last by convention even in the
    * (unreachable in practice) case of ≥99 definitions — the file names still
    * differ, so nothing is overwritten.
    */
  def entries(manifest: PipelineManifest, options: Options = Options()): List[Entry] =
    val create = PipelineProtoEncoder.createDataflowGraph(
      defaultCatalog = options.defaultCatalog,
      defaultDatabase = options.defaultDatabase,
      sqlConf = options.sqlConf,
    )
    val definitions = PipelineProtoEncoder.definitions(GraphIdPlaceholder, manifest)
    val start = PipelineProtoEncoder.startRun(
      GraphIdPlaceholder,
      options.dry,
      options.storage,
      fullRefreshAll = options.fullRefresh,
    )

    val head = entry("00-create-dataflow-graph", create)
    val body = definitions.zipWithIndex.map { (command, i) =>
      entry(f"${i + 1}%02d-${describe(command)}", command)
    }
    val tail = entry("99-start-run", start)
    head :: body ::: List(tail)

  /** Write the dump into `directory`, creating it if needed, and return the
    * files written in send order.
    *
    * Stale files from a previous dump of a DIFFERENT pipeline are removed
    * first: a directory that mixes two pipelines' commands is worse than no
    * dump at all, because the reader has no way to tell which is which. Only
    * `*.txtpb` files are touched — this never deletes anything it did not write.
    */
  def writeTo(directory: Path, dump: List[Entry]): List[Path] =
    Files.createDirectories(directory)
    if Files.isDirectory(directory) then
      val stale = Files.list(directory)
      try
        stale.iterator.asScala
          .filter(p => p.getFileName.toString.endsWith(s".$Extension"))
          .toList
          .foreach(Files.deleteIfExists(_))
      finally stale.close()
    dump.map { e =>
      val path = directory.resolve(e.fileName)
      Files.write(path, e.prototext.getBytes(UTF_8))
      path
    }

  /** Render one command as protobuf text format, with a comment header naming
    * the file.
    *
    * `TextFormat.printer()` and NOT `debugFormatPrinter()`/`toString`: the
    * debug printer redacts fields and deliberately perturbs whitespace to
    * discourage byte comparison, which is exactly the property this dump
    * needs. The `#` lines are text-format comments and are skipped by
    * `TextFormat.merge`, so the round-trip law still holds (asserted).
    */
  private def entry(stem: String, command: sc.PipelineCommand): Entry =
    val fileName = s"$stem.$Extension"
    val body     = TextFormat.printer().printToString(command)
    val text =
      s"""|# sdp wire dump — $fileName
          |# protobuf text format of one spark.connect.PipelineCommand, exactly as
          |# sdpRun/sdpDryRun sends it. The dataflow graph id is server-assigned;
          |# it is rendered here as "$GraphIdPlaceholder".
          |$body""".stripMargin
    Entry(fileName, command, text)

  /** The file-name stem for one command: its kind plus the thing it names. */
  private def describe(command: sc.PipelineCommand): String =
    if command.hasDefineOutput then s"define-output-${slug(command.getDefineOutput.getOutputName)}"
    else if command.hasDefineFlow then s"define-flow-${slug(command.getDefineFlow.getFlowName)}"
    else command.getCommandTypeCase.name.toLowerCase.replace('_', '-')

  /** Dataset and flow names are author-chosen and may carry anything the
    * manifest's percent-encoding allows; a file name may not. Everything
    * outside `[A-Za-z0-9._-]` becomes `_`. */
  private def slug(name: String): String =
    val cleaned = name.map(c => if c.isLetterOrDigit || c == '.' || c == '-' || c == '_' then c else '_')
    if cleaned.isEmpty then "unnamed" else cleaned
