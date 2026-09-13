package dev.sdp.connect

import dev.sdp.core.{FlowDetails, PipelineManifest, PipelineNode}
import org.apache.spark.connect.proto as sc

/** Thrown when the manifest carries a construct this encoder cannot put on the
  * pinned wire. `validate`/`manifest` accept such a pipeline offline, so the
  * author meets it at `run`/`dry-run` — and must read a sentence, not a defect
  * trace ([[PipelinesRegistration.RegistrationError.UnsupportedWire]] carries
  * it into the typed channel).
  *
  * AUTO CDC used to live here; since roadmap S1 its SCD1 half is encoded
  * unconditionally and *server* version safety is [[VersionGate]]'s job. Two
  * constructs use this today:
  *   - a subquery inside a standalone expression, which is genuinely
  *     unrepresentable ([[AlgebraProtoEncoder.expression]]);
  *   - **AUTO CDC stored as SCD type 2** (roadmap S2), which the pinned proto
  *     cannot express — see [[Scd2Wire]], which decides by descriptor, so the
  *     refusal disappears on its own when the dependency carries the fields. */
final class UnsupportedWireFeature(message: String) extends RuntimeException(message)

/** Pure translation from the canonical [[PipelineManifest]] to the Spark
  * Declarative Pipelines wire protocol (`spark/connect/pipelines.proto`).
  *
  * Message names and field semantics were read from the canonical proto in
  * the Spark source tree — see ROADMAP "Protobuf toolchain: decided". The
  * notable mapping facts:
  *
  *   - There is no streaming-table output type on the wire. Streaming-ness
  *     lives in the *flow*: `StreamingTable` encodes as `OutputType.TABLE`
  *     plus flows whose `Read` relations set `is_streaming = true`.
  *   - `MaterializedView`/`TemporaryView` carry their SQL as their single
  *     flow (`Relation.sql`); lineage *into* them is embodied by that SQL,
  *     so manifest edges targeting them produce no additional flows.
  *   - Until the typed algebra lands (Phase 6), `Table`/`StreamingTable`
  *     flows encode graph *shape*: one flow per lineage edge, reading the
  *     upstream via `Read.named_table`.
  *
  * Everything here is deterministic: the manifest is canonically sorted and
  * the encoder preserves that order, so equal manifests yield byte-identical
  * command sequences.
  */
object PipelineProtoEncoder:

  /** Step 1 of the registration sequence: ask the server for a graph.
    * `sqlConf` entries apply to every flow in the graph (e.g.
    * `pipelines.incompatibleViewCheck.enabled` — the server's own escape
    * hatch for streaming reads over batch views).
    */
  def createDataflowGraph(
      defaultCatalog: Option[String] = None,
      defaultDatabase: Option[String] = None,
      sqlConf: Map[String, String] = Map.empty,
  ): sc.PipelineCommand =
    val builder = sc.PipelineCommand.CreateDataflowGraph.newBuilder()
    defaultCatalog.foreach(builder.setDefaultCatalog)
    defaultDatabase.foreach(builder.setDefaultDatabase)
    sqlConf.toList.sortBy(_._1).foreach((k, v) => builder.putSqlConf(k, v))
    sc.PipelineCommand.newBuilder().setCreateDataflowGraph(builder).build()

  /** Step 2: every `DefineOutput` and `DefineFlow` for the manifest, in
    * deterministic order (outputs in manifest node order; then authored
    * flows in manifest flow order; then shape-derived flows for nodes
    * without authored ones).
    *
    * Authored flows (manifest v2) carry real relation trees and take
    * precedence: a node with at least one authored flow gets no
    * shape-derived `Read.named_table` flows — its lineage edges exist
    * because of the authored relation in the first place.
    */
  def definitions(graphId: String, manifest: PipelineManifest): List[sc.PipelineCommand] =
    // External tables are read from the catalog, never registered — they
    // produce no DefineOutput (and no flow); the server resolves them as
    // usedExternalInputs of the flows that read them.
    val outputs = manifest.nodes.flatMap(defineOutput(graphId, _))

    val authoredTargets = manifest.flows.map(_.target).toSet
    // Disjoint plan-id ranges per flow (10k apart — far above any real plan's
    // node count): ids must not collide across flows in one graph, see
    // AlgebraProtoEncoder.relation.
    val authored = manifest.flows.zipWithIndex.map { (flow, i) =>
      flow.details match
        case FlowDetails.WriteRelation(rel) =>
          flowCommand(
            graphId,
            flow.name,
            flow.target,
            AlgebraProtoEncoder.relation(rel, planIdBase = i.toLong * 10000L),
            flow.once,
          )
        case _: FlowDetails.AutoCdc =>
          autoCdcFlowCommand(graphId, flow)
    }
    val derived = manifest.nodes
      .filterNot(n => authoredTargets.contains(n.id))
      .flatMap(defineFlows(graphId, _, manifest))

    outputs ++ authored ++ derived

  /** Step 3: start the run. `dry = true` is server-side validation only —
    * the cheapest end-to-end correctness check a build can ask for.
    *
    * `fullRefreshAll` sets `StartRun.full_refresh_all` (field 3), the "rebuild
    * everything" button: the server rolls every resettable streaming flow's
    * checkpoint into a new numbered sibling BEFORE materialization
    * (`State.reset`, v4.2.0) and materialization then wipes the targets, so the
    * graph recomputes from its sources. The field is written ONLY when asked —
    * absence is the default, matching `createDataflowGraph`'s treatment of the
    * optional graph defaults, so an ordinary run's bytes are unchanged.
    *
    * NOT here on purpose: `full_refresh_selection` (field 2) and
    * `refresh_selection` (field 4), the *selective* forms. They carry server-side
    * validation rules a client must mirror (no subset together with
    * `full_refresh_all`; no overlap between the two lists; selections are NOT
    * transitively expanded, so an unselected upstream is excluded rather than
    * recomputed) — see `BehaviorInventory` row FR-3. A selective surface is its
    * own feature; this one is the all-or-nothing button.
    *
    * @param storage checkpoint/metadata root; the server requires an
    *                absolute URI with a scheme (`file://`, `s3a://`, ...)
    */
  def startRun(
      graphId: String,
      dry: Boolean,
      storage: String,
      fullRefreshAll: Boolean = false,
  ): sc.PipelineCommand =
    val run = sc.PipelineCommand.StartRun
      .newBuilder()
      .setDataflowGraphId(graphId)
      .setDry(dry)
      .setStorage(storage)
    if fullRefreshAll then { val _ = run.setFullRefreshAll(true) }
    sc.PipelineCommand.newBuilder().setStartRun(run).build()

  /** The server's own undo for step 1: drop the graph and stop anything
    * attached to it (`pipelines.proto` field 4 — `DropDataflowGraph`, handled
    * by `PipelinesHandler` since the 4.1.0 RELEASE, verified in `../spark`).
    *
    * Used only on the abort path: once `CreateDataflowGraph` has succeeded, a
    * later `DefineFlow` rejection would otherwise leave a half-populated graph
    * in the server's `DataflowGraphRegistry` forever — one leaked entry per
    * failed `~sdpDryRun` save. See `PipelinesRegistration.defineAll`.
    */
  def dropDataflowGraph(graphId: String): sc.PipelineCommand =
    sc.PipelineCommand
      .newBuilder()
      .setDropDataflowGraph(
        sc.PipelineCommand.DropDataflowGraph.newBuilder().setDataflowGraphId(graphId)
      )
      .build()

  // ------------------------------------------------------------------
  // outputs
  // ------------------------------------------------------------------

  private def defineOutput(graphId: String, node: PipelineNode): Option[sc.PipelineCommand] =
    val builder = sc.PipelineCommand.DefineOutput
      .newBuilder()
      .setDataflowGraphId(graphId)
      .setOutputName(node.id)

    val configured = node match
      case PipelineNode.Table(_, format) =>
        Some(tableOutput(builder, format))
      case PipelineNode.StreamingTable(_, format) =>
        // No STREAMING_TABLE on the wire: TABLE + streaming flows.
        Some(tableOutput(builder, format))
      case PipelineNode.MaterializedView(_, _) =>
        Some(builder.setOutputType(sc.OutputType.MATERIALIZED_VIEW))
      case PipelineNode.TemporaryView(_, _) =>
        Some(builder.setOutputType(sc.OutputType.TEMPORARY_VIEW))
      case PipelineNode.ExternalTable(_) =>
        None // external/source table — not a managed output

    configured.map(b => sc.PipelineCommand.newBuilder().setDefineOutput(b).build())

  /** Delta is SDP's implicit default provider — the catalog supplies it when no
    * format is sent, so it must never go on the wire (this mirrors the official
    * SDP, whose `Table.format` is `Option` and is `None` unless a user writes an
    * explicit `USING`). Tracks the DSL's own default format. */
  private val DefaultTableFormat = "delta"

  /** Build a TABLE output, emitting `format` **only when it is a genuinely
    * non-default provider**.
    *
    * Why: SDP's `DatasetManager.materializeTable` re-applies every resolved
    * property on each re-run's `alterTable`, and a set `format` makes the
    * server add the reserved `provider` (`resolveTableProperties` →
    * `PROP_PROVIDER`). Delta's `AlterTableSetPropertiesDeltaCommand` rejects
    * `provider` in *any* ALTER **unconditionally** (decompiled: the check sees
    * only the key, never the value — so even an unchanged `delta`→`delta` set
    * throws `DELTA_CANNOT_CHANGE_PROVIDER`). The official SDP avoids this by
    * leaving `format = None` on the common path; we do the same. The server
    * reads the format by proto presence (`Option.when(tableDetails.hasFormat)`),
    * so not setting it leaves `table.format = None` → no `provider` → re-runs
    * succeed, and the table is still Delta via the catalog default. A genuinely
    * non-default format (parquet/iceberg) is still emitted — and carries the
    * same upstream re-run limitation an explicit `USING <fmt>` has in Python. */
  private def tableOutput(
      builder: sc.PipelineCommand.DefineOutput.Builder,
      format: String,
  ): sc.PipelineCommand.DefineOutput.Builder =
    val out = builder.setOutputType(sc.OutputType.TABLE)
    if format.nonEmpty && format != DefaultTableFormat then
      val _ = out.setTableDetails(
        sc.PipelineCommand.DefineOutput.TableDetails.newBuilder().setFormat(format)
      )
    out

  // ------------------------------------------------------------------
  // flows
  // ------------------------------------------------------------------

  private def defineFlows(
      graphId: String,
      node: PipelineNode,
      manifest: PipelineManifest,
  ): List[sc.PipelineCommand] =
    node match
      case PipelineNode.MaterializedView(id, sql) =>
        List(flowCommand(graphId, flowName = id, target = id, relation = sqlRelation(sql)))

      case PipelineNode.TemporaryView(id, sql) =>
        List(flowCommand(graphId, flowName = id, target = id, relation = sqlRelation(sql)))

      case PipelineNode.Table(id, _) =>
        // SDP semantics (learned from the live 4.1.2 analyzer): on the wire,
        // OutputType.TABLE IS a streaming table — batch relations can't feed
        // it directly, and DefineFlow.once (the proto's escape hatch) is
        // rejected by the 4.1.2 server (DEFINE_FLOW_ONCE_OPTION_NOT_SUPPORTED).
        // Shape-only batch reads are still emitted here so lineage round-trips;
        // semantically valid table feeds need real relations (STREAM(...) /
        // streaming sources) — that arrives with the F11 algebra.
        readFlows(graphId, id, manifest, streaming = false, once = false)

      case PipelineNode.StreamingTable(id, _) =>
        readFlows(graphId, id, manifest, streaming = true, once = false)

      case PipelineNode.ExternalTable(_) =>
        Nil // external/source tables have no flow — read from the catalog

  /** Shape-honest flows for lineage edges: one flow per upstream. A single
    * upstream uses the dataset's own name as the flow name (the implicit
    * default flow); fan-in disambiguates deterministically.
    */
  private def readFlows(
      graphId: String,
      target: String,
      manifest: PipelineManifest,
      streaming: Boolean,
      once: Boolean,
  ): List[sc.PipelineCommand] =
    val upstreams = manifest.edges.collect { case e if e.to == target => e.from }
    upstreams match
      case Nil => Nil
      case single :: Nil =>
        List(flowCommand(graphId, target, target, readRelation(single, streaming), once))
      case many =>
        many.map { up =>
          flowCommand(graphId, s"${target}__from__$up", target, readRelation(up, streaming), once)
        }

  private def flowCommand(
      graphId: String,
      flowName: String,
      target: String,
      relation: sc.Relation,
      once: Boolean = false,
  ): sc.PipelineCommand =
    val flow = sc.PipelineCommand.DefineFlow
      .newBuilder()
      .setDataflowGraphId(graphId)
      .setFlowName(flowName)
      .setTargetDatasetName(target)
      .setRelationFlowDetails(
        sc.PipelineCommand.DefineFlow.WriteRelationFlowDetails
          .newBuilder()
          .setRelation(relation)
      )
    // `once` is set ONLY when true, never `setOnce(false)`: the server checks
    // PRESENCE (`if (flow.hasOnce) throw DEFINE_FLOW_ONCE_OPTION_NOT_SUPPORTED`
    // — PipelinesHandler.defineFlow, still true at 4.2.0 and at master HEAD), so
    // an explicit false would fail every ordinary flow. See the behavioral
    // matrix rows ONCE-1 / ONCE-2 (BehaviorInventory); asserted by
    // PipelineProtoEncoderSpec ("a once=false flow leaves the field ABSENT").
    if once then { val _ = flow.setOnce(true) }
    sc.PipelineCommand.newBuilder().setDefineFlow(flow).build()

  /** AUTO CDC flow → the wire (`DefineFlow.auto_cdc_flow_details`, field 10).
    *
    * Ungated since roadmap S1: the pinned `spark-connect-common 4.2.0` carries
    * `AutoCdcFlowDetails`, so the encode is unconditional and **version safety
    * is the [[VersionGate]] handshake's job** — a sub-4.2 server is refused
    * before `CreateDataflowGraph`, because proto3 would silently strip this
    * branch and leave the server with a detail-less `DefineFlow`.
    *
    * Every SCD1-era field of the published 4.2.0 message is emitted (field
    * numbers per [[FlowDetails.AutoCdc]]). Two server-side facts worth carrying
    * here rather than rediscovering:
    *
    *   - `apply_as_truncates` and the two `ignore_null_updates_*` lists are
    *     declared on the message but **not yet honored by the 4.2.0 engine**
    *     (`PipelinesHandler.buildAutoCdcFlow`, TODO SPARK-57092 / SPARK-57093).
    *     We send them because the author asked for them and the wire accepts
    *     them; the engine will start honoring them without a client change.
    *   - `keys`, `column_list` and `except_column_list` must resolve to plain
    *     column identifiers — the server maps each through
    *     `asUnqualifiedColumnName` and fails `AUTOCDC_NON_COLUMN_IDENTIFIER`
    *     otherwise. `sequence_by` and the conditions are ordinary expressions.
    *
    * **SCD2** (`SCD_TYPE_2` + the track-history lists 11/12) is staged but
    * GATED — roadmap S2. The pinned 4.2.0 proto has none of those fields, so an
    * SCD2 flow is refused here with [[UnsupportedWireFeature]] (a sentence
    * naming the pin it needs), never half-encoded: proto3 would otherwise send
    * `SCD_TYPE_UNSPECIFIED` — which the server reads as SCD *1* — and silently
    * drop the history-tracking lists, i.e. materialize the wrong table shape.
    * The condition is a descriptor lookup ([[Scd2Wire]]), so the encode turns
    * itself on when the artifact carries the fields.
    */
  private def autoCdcFlowCommand(graphId: String, flow: dev.sdp.core.Flow): sc.PipelineCommand =
    val cdc = flow.details match
      case c: FlowDetails.AutoCdc => c
      case other =>
        throw new IllegalStateException(s"autoCdcFlowCommand called with $other")

    val ex = AlgebraProtoEncoder.expression(_)

    // GATE(spark-4.3) — refuse BEFORE building anything, so a gated pipeline
    // cannot leave a half-formed command behind. The trigger is the SCD2
    // construct in any of its spellings: the type itself, or a track-history
    // list (which the validator only lets through under SCD2, but a manifest
    // can also arrive from a file).
    val usesScd2 =
      cdc.scdType == dev.sdp.core.ScdType.Scd2 ||
        cdc.trackHistoryColumnList.nonEmpty ||
        cdc.trackHistoryExceptColumnList.nonEmpty
    if usesScd2 && !Scd2Wire.available then
      throw Scd2Wire.unsupported(flow.name, flow.target)

    val ac = sc.PipelineCommand.DefineFlow.AutoCdcFlowDetails.newBuilder()
    ac.setSource(cdc.source)                                                  // 1
    cdc.keys.foreach(k => ac.addKeys(ex(k)))                                  // 2
    ac.setSequenceBy(ex(cdc.sequenceBy))                                      // 3
    cdc.applyAsDeletes.foreach(e => ac.setApplyAsDeletes(ex(e)))              // 6
    cdc.applyAsTruncates.foreach(e => ac.setApplyAsTruncates(ex(e)))          // 7
    cdc.columnList.foreach(e => ac.addColumnList(ex(e)))                      // 8
    cdc.exceptColumnList.foreach(e => ac.addExceptColumnList(ex(e)))          // 9
    cdc.scdType match                                                         // 10
      case dev.sdp.core.ScdType.Scd1 =>
        ac.setStoredAsScdType(sc.PipelineCommand.DefineFlow.SCDType.SCD_TYPE_1)
      case dev.sdp.core.ScdType.Scd2 =>
        // 10 = SCD_TYPE_2, plus fields 11/12 — all three by descriptor, since
        // the pinned artifact has no symbol for any of them (see Scd2Wire).
        Scd2Wire.encode(
          ac,
          cdc.trackHistoryColumnList.map(ex),
          cdc.trackHistoryExceptColumnList.map(ex),
        )
    cdc.ignoreNullUpdatesColumnList.foreach(e => ac.addIgnoreNullUpdatesColumnList(ex(e)))             // 14
    cdc.ignoreNullUpdatesExceptColumnList.foreach(e => ac.addIgnoreNullUpdatesExceptColumnList(ex(e))) // 15

    val df = sc.PipelineCommand.DefineFlow
      .newBuilder()
      .setDataflowGraphId(graphId)
      .setFlowName(flow.name)
      .setTargetDatasetName(flow.target)
      .setAutoCdcFlowDetails(ac)
    // Presence, not value — see the note on flowCommand above (ONCE-1/ONCE-2).
    if flow.once then { val _ = df.setOnce(true) }
    sc.PipelineCommand.newBuilder().setDefineFlow(df).build()

  private def readRelation(upstream: String, streaming: Boolean): sc.Relation =
    sc.Relation
      .newBuilder()
      .setRead(
        sc.Read
          .newBuilder()
          .setNamedTable(sc.Read.NamedTable.newBuilder().setUnparsedIdentifier(upstream))
          .setIsStreaming(streaming)
      )
      .build()

  private def sqlRelation(query: String): sc.Relation =
    sc.Relation
      .newBuilder()
      .setSql(sc.SQL.newBuilder().setQuery(query))
      .build()
