package dev.sdp.connect

import com.google.protobuf.Descriptors
import org.apache.spark.connect.proto as sc

/** GATE(spark-4.3) — roadmap S2: SCD type 2's wire representation, detected on
  * the **pinned artifact's descriptor** rather than guessed from a version
  * string.
  *
  * The situation this exists for: `SCD_TYPE_2 = 2` and
  * `AutoCdcFlowDetails.track_history_column_list` (11) /
  * `track_history_except_column_list` (12) are master-only
  * (SPARK-58247, 2026-07-22 — `git tag --contains` names no release), and the
  * published `spark-connect-common 4.2.0` carries neither. So an SCD2 pipeline
  * is fully authorable, validatable and renderable offline, and simply cannot
  * be *encoded* until the dependency bumps to a Spark release whose proto
  * carries them.
  *
  * **Why a descriptor lookup and not a version constant.** Three properties
  * fall out of asking the artifact instead of asking a string:
  *   - the gate flips **by itself** the moment `sdp.connect.common.version`
  *     moves to a proto carrying SCD2 — no code edit, no release coordination,
  *     and no risk of a constant that says 4.3 when upstream shipped it in
  *     4.2.1 or 5.0;
  *   - a vendor or snapshot artifact that carries the fields early works
  *     immediately, and one that carries them late is refused honestly;
  *   - the refusal and the encode read the SAME source of truth, so they can
  *     never disagree.
  *
  * **Why the encode below goes through the reflective protobuf API.** This code
  * must COMPILE against the pinned artifact, where the constant
  * `SCDType.SCD_TYPE_2` and the two generated `addTrackHistory…` builders do not
  * exist. `setStoredAsScdTypeValue(2)` (generated for every proto3 enum field,
  * by number) and `Message.Builder.addRepeatedField(FieldDescriptor, value)`
  * are both stable protobuf-java surface that says the same bytes without
  * naming a symbol the pinned jar lacks. Nothing here is speculative about
  * shapes: `stored_as_scd_type` is an ordinary enum field and 11/12 are
  * `repeated Expression`, exactly like `column_list`.
  *
  * Server-version safety is a *separate* concern and stays where it belongs:
  * [[VersionGate]] refuses a server too old for the construct even when our own
  * artifact could encode it.
  */
private[connect] object Scd2Wire:

  /** The `SCD_TYPE_2` enum value, if the pinned proto has it. */
  private lazy val scd2Value: Option[Descriptors.EnumValueDescriptor] =
    Option(sc.PipelineCommand.DefineFlow.SCDType.getDescriptor.findValueByName("SCD_TYPE_2"))

  /** Field 11 — `track_history_column_list`, if the pinned proto has it. */
  lazy val trackHistoryField: Option[Descriptors.FieldDescriptor] =
    field("track_history_column_list")

  /** Field 12 — `track_history_except_column_list`, if the pinned proto has it. */
  lazy val trackHistoryExceptField: Option[Descriptors.FieldDescriptor] =
    field("track_history_except_column_list")

  private def field(name: String): Option[Descriptors.FieldDescriptor] =
    Option(sc.PipelineCommand.DefineFlow.AutoCdcFlowDetails.getDescriptor.findFieldByName(name))

  /** True when the pinned artifact can express SCD2 in full: the enum value AND
    * both history-tracking lists. All-or-nothing on purpose — encoding the SCD
    * type while silently dropping a track-history list the author wrote would be
    * the exact proto3 failure mode this gate exists to prevent. */
  def available: Boolean =
    scd2Value.isDefined && trackHistoryField.isDefined && trackHistoryExceptField.isDefined

  /** The refusal, as a sentence an author can act on. Names the flow, what is
    * missing, what still works offline, and what changes it. */
  def unsupported(flowName: String, target: String): UnsupportedWireFeature =
    new UnsupportedWireFeature(
      s"AUTO CDC flow '$flowName' (target '$target') is stored as SCD type 2, which the pinned " +
        "Spark Connect proto cannot express: SCDType.SCD_TYPE_2 and AutoCdcFlowDetails." +
        "track_history_column_list (11) / track_history_except_column_list (12) are master-only " +
        "upstream (SPARK-58247) and absent from spark-connect-common 4.2.0. Authoring, " +
        "validation and `sdpManifest` work offline today; to RUN it, build against a Spark " +
        "release whose proto carries those fields (spark-connect-common 4.3.0 or newer, " +
        "whichever release ships SPARK-58247 first) — the encoder detects them by descriptor, " +
        "so nothing here changes when the pin moves. Until then, storedAsScdType = 1 runs on any " +
        "Spark 4.2+ server."
    )

  /** Put the SCD2 half of `AutoCdcFlowDetails` on the builder. Call only when
    * [[available]]; the caller refuses first, so this is total here. */
  def encode(
      builder: sc.PipelineCommand.DefineFlow.AutoCdcFlowDetails.Builder,
      trackHistory: List[sc.Expression],
      trackHistoryExcept: List[sc.Expression],
  ): Unit =
    // The enum by NUMBER: setStoredAsScdTypeValue exists on the pinned builder
    // (proto3 generates it for every enum field) and writes field 10 = 2.
    val _ = builder.setStoredAsScdTypeValue(2)
    trackHistoryField.foreach(fd => trackHistory.foreach(e => builder.addRepeatedField(fd, e)))
    trackHistoryExceptField.foreach(fd =>
      trackHistoryExcept.foreach(e => builder.addRepeatedField(fd, e))
    )
