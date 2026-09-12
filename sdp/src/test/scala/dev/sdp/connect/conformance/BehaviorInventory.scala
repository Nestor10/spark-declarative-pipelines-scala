package dev.sdp.connect.conformance

/** The SDP **behavioral** conformance matrix — the D9 fix.
  *
  * `ConnectInventory`/`SupportedCapabilities` inventory the Spark Connect
  * *wire surface*: which `Relation`/`Expression`/`PipelineCommand` fields we
  * emit. That matrix read 100% on T0 while `externalTable` was entirely
  * missing, because reading a table you do not own is not a message — it is a
  * *usage distinction* in `Read.NamedTable` plus the *absence* of a
  * `DefineOutput`. You cannot inventory a message you are correctly not
  * sending. D9's durable lesson: **wire coverage says nothing about semantic
  * completeness**, and every expensive bug in this project's history has lived
  * exactly there — graph-defaults edge drop, TRUNCATE-on-Delta, provider
  * re-assert on re-run, re-run NULL semantics.
  *
  * So this file inventories the other layer: behaviors of
  * `PipelinesHandler`/`DatasetManager`/`TriggeredGraphExecution` that an SDP
  * client depends on. Each row carries a Spark **source anchor** (the authority
  * is `../spark`, not our memory) and an honest coverage status.
  *
  * Rules, so this stays a measurement and not a brochure:
  *   - **Every row cites a source anchor.** `BehaviorCoverageSpec` fails otherwise.
  *   - **`Covered` must name a spec class that exists** on the test classpath —
  *     checked reflectively, so a renamed or deleted spec breaks the claim loudly.
  *   - **Uncovered rows stay.** An honest gap is the entire point; padding the
  *     matrix would recreate D9 one layer up.
  *   - Behavior verified by hand (a live session, a measured experiment) is
  *     **still `Uncovered`** — with the receipt in `note`. Only an automated
  *     spec counts as coverage.
  *
  * Anchors are relative to the Spark checkout (`../spark`), read at master
  * HEAD 919f0808549 (2026-09-12).
  */
object BehaviorInventory:

  /** What a behavior is about. Ordering here is the report's ordering: the
    * registration handshake first (you cannot get anywhere without it), then
    * the semantics that bite after the second run. */
  enum Area(val title: String):
    case Registration   extends Area("registration sequence + error surfaces")
    case GraphDefaults  extends Area("graph defaults (catalog/database) and name resolution")
    case Materialization extends Area("dataset materialization: fresh vs re-run")
    case FullRefresh    extends Area("full refresh / reset semantics")
    case OnceFlows      extends Area("once (backfill) flow semantics")
    case ExternalInputs extends Area("external input resolution")
    case Streaming      extends Area("streaming trigger + run termination")
    case Events         extends Area("event / progress message shapes")
    case DryRun         extends Area("dry run (StartRun dry=true)")

  /** Coverage status. Deliberately only three — "we tried it once by hand" is
    * not a status, it is a `note` on an `Uncovered` row. */
  enum Coverage:
    /** A spec asserts this behavior. `spec` is a fully-qualified class name and
      * must resolve on the test classpath. */
    case Covered(spec: String, how: String)
    /** Nothing automated asserts it. `note` records what we *do* know. */
    case Uncovered(note: String = "")
    /** Structurally cannot apply to this client. `reason` is mandatory. */
    case NotApplicable(reason: String)

    def label: String = this match
      case Covered(spec, _)    => s"Covered(${spec.split('.').last})"
      case Uncovered(_)        => "Uncovered"
      case NotApplicable(_)    => "NotApplicable"

  /** One behavior of the server that this client depends on.
    *
    * @param id       stable identifier — referenced by commits/issues, so it
    *                 never changes once published
    * @param area     grouping for the report
    * @param what     the observable behavior, one line
    * @param anchor   `path → Class.method` in the Spark source tree
    * @param coverage honest status
    * @param matchable literal strings a client matches on (error classes, event
    *                  wording, enum values) — empty when there are none
    */
  final case class Behavior(
      id: String,
      area: Area,
      what: String,
      anchor: String,
      coverage: Coverage,
      matchable: List[String] = Nil,
  )

  // Spec class names, written once: the drift guard resolves each of these
  // reflectively, so a rename cannot leave a stale claim behind.
  private val RegistrationIT = "dev.sdp.connect.PipelinesRegistrationIntegrationSpec"
  private val RunProgressS   = "dev.sdp.core.RunProgressSpec"
  private val VersionGateS   = "dev.sdp.connect.VersionGateSpec"

  private def reg(id: String, what: String, anchor: String, coverage: Coverage, m: List[String] = Nil) =
    Behavior(id, Area.Registration, what, anchor, coverage, m)

  val rows: List[Behavior] = List(
    // ---------------------------------------------------------------- registration
    reg(
      "REG-1-command-envelope",
      "every PipelineCommand rides on ExecutePlan as Command.pipeline_command; the reply is one pipeline_command_result (preceded, for StartRun, by a stream of pipeline_event_result)",
      "sql/connect/server/.../connect/planner/SparkConnectPlanner.scala → SparkConnectPlanner.handlePipelineCommand",
      Coverage.Covered(RegistrationIT, "drives the real sequence over gRPC and reads the result off the response stream"),
      List("PipelineCommandResult", "PipelineEventResult"),
    ),
    reg(
      "REG-2-graph-id",
      "CreateDataflowGraph returns a server-generated UUID; graphs live in a per-session map with no persistence and no TTL, and every later command must carry the id",
      "sql/connect/server/.../connect/pipelines/DataflowGraphRegistry.scala → DataflowGraphRegistry.createDataflowGraph",
      Coverage.Covered(RegistrationIT, "asserts a non-empty graph id comes back and is used by the subsequent commands"),
    ),
    reg(
      "REG-3-unknown-graph",
      "a command naming an unknown (or session-expired) graph id is refused",
      "sql/connect/server/.../connect/pipelines/DataflowGraphRegistry.scala → DataflowGraphRegistry.getDataflowGraphOrThrow",
      Coverage.Uncovered("we create, define and run inside one session and never reuse an id, so the path is unexercised"),
      List("DATAFLOW_GRAPH_NOT_FOUND", "KD011"),
    ),
    reg(
      "REG-4-output-before-flow",
      "DefineFlow qualifies names by consulting ALREADY-registered views/sinks, so a temp-view or sink output must be defined before the flow that writes it",
      "sql/connect/server/.../connect/pipelines/PipelinesHandler.scala → PipelinesHandler.defineFlow (flowWritesToView / isImplicitFlowForTempView)",
      Coverage.Uncovered("satisfied by construction — PipelineProtoEncoder.definitions emits `outputs ++ authored ++ derived` — but no spec pins the invariant"),
      List("PIPELINE_DATASET_WITHOUT_FLOW"),
    ),
    reg(
      "REG-5-flow-name-shape",
      "a flow is implicit iff flow_name is string-equal to target_dataset_name; a non-implicit flow name must be single-part",
      "sql/connect/server/.../connect/pipelines/PipelinesHandler.scala → PipelinesHandler.defineFlow (isImplicitFlow)",
      Coverage.Uncovered("our flow names are single-part by construction; nothing asserts the server's rule"),
      List("MULTIPART_FLOW_NAME_NOT_SUPPORTED"),
    ),
    reg(
      "REG-6-duplicates-late",
      "duplicate output/flow identifiers are rejected at StartRun (graph assembly), NOT at DefineOutput/DefineFlow time — a client gets no early signal",
      "sql/pipelines/.../graph/GraphRegistrationContext.scala → GraphRegistrationContext.toDataflowGraph (assertNoDuplicates)",
      Coverage.Uncovered("our offline assembly rejects duplicate datasets before anything is sent (GraphValidation), so we never observe the server's verdict"),
      List("PIPELINE_DUPLICATE_IDENTIFIERS.OUTPUT", "PIPELINE_DUPLICATE_IDENTIFIERS.FLOW"),
    ),
    reg(
      "REG-7-empty-pipeline",
      "a graph with no table, persisted view or sink is rejected at StartRun — temporary views alone are not a pipeline",
      "sql/pipelines/.../graph/GraphRegistrationContext.scala → GraphRegistrationContext.toDataflowGraph (isEmpty)",
      Coverage.Uncovered("nothing asserts it; our validator does not yet reject a temp-view-only pipeline offline"),
      List("RUN_EMPTY_PIPELINE"),
    ),
    reg(
      "REG-8-storage-required",
      "StartRun refuses an empty storage, and the storage root must be an absolute URI with a scheme",
      "sql/connect/server/.../connect/pipelines/PipelinesHandler.scala → PipelinesHandler.startRun; sql/pipelines/.../graph/PipelineUpdateContextImpl.scala → PipelineUpdateContextImpl.validateStorageRoot",
      Coverage.Uncovered("our default is file:///tmp/sdp/<name> and the requirement is documented; no spec asserts the server's refusal of a bad root"),
      List("Storage must be specified to start a run.", "PIPELINE_STORAGE_ROOT_INVALID"),
    ),
    reg(
      "REG-9-details-required",
      "DefineFlow.details must be relation_flow_details or auto_cdc_flow_details — an unset oneof is refused, which is exactly what proto3 leaves behind when a 4.2-only message reaches a 4.1 server",
      "sql/connect/server/.../connect/pipelines/PipelinesHandler.scala → PipelinesHandler.defineFlow (getDetailsCase)",
      Coverage.Covered(VersionGateS, "the version handshake refuses an AUTO CDC pipeline against a sub-4.2 server before any command is sent"),
      List("Unsupported DefineFlow details case"),
    ),
    reg(
      "REG-10-flow-body-analysis",
      "a client that evaluates flow bodies SERVER-side must set PipelineAnalysisContext in UserContext.extensions, and only a whitelist of spark.sql commands is allowed inside a flow function",
      "sql/connect/server/.../connect/utils/PipelineAnalysisContextUtils.scala → PipelineAnalysisContextUtils.isInsidePipelineFlowFunction; PipelinesHandler.blockUnsupportedSqlCommand",
      Coverage.NotApplicable("D10: flow bodies are evaluated CLIENT-side into a Rel algebra and sent as a finished relation — we never ask the server to run a query function"),
      List("ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION", "UNSUPPORTED_PIPELINE_SPARK_SQL_COMMAND"),
    ),
    reg(
      "REG-11-unimplemented-commands",
      "ExecuteOutputFlows / GetQueryFunctionExecutionSignalStream / DefineFlowQueryFunctionResult exist in pipelines.proto but are NOT handled on master — they fail as unsupported",
      "sql/connect/server/.../connect/pipelines/PipelinesHandler.scala → PipelinesHandler.handlePipelinesCommand",
      Coverage.NotApplicable("we emit only the four commands claimed in SupportedCapabilities.pipelineCommands; the deferred query-function handshake is not a surface we use"),
    ),

    // ------------------------------------------------------------- graph defaults
    Behavior(
      "DEF-1-fallback-mutates-session",
      Area.GraphDefaults,
      "absent default_catalog/default_database fall back to the session's CURRENT catalog/database — and then the server SETS them on the session, so graph creation mutates session state",
      "sql/connect/server/.../connect/pipelines/PipelinesHandler.scala → PipelinesHandler.createDataflowGraph",
      Coverage.Uncovered("measured by hand 2026-06-13; the client now sends both whenever configured (sdpDefaultCatalog/Database, SDP_DEFAULT_*)"),
      List("No default catalog was supplied. Falling back to the current catalog:"),
    ),
    Behavior(
      "DEF-2-four-consumers",
      Area.GraphDefaults,
      "the captured defaults qualify four independent things: DefineOutput identifiers, flow identifiers, the flow's read-resolution QueryContext, and StartRun refresh selections",
      "sql/connect/server/.../connect/pipelines/PipelinesHandler.scala → PipelinesHandler.defineOutput / defineFlow / createTableFilters",
      Coverage.Uncovered("no spec exercises a named V2 catalog end to end"),
    ),
    Behavior(
      "DEF-3-edge-drop",
      Area.GraphDefaults,
      "a read whose qualified identifier is not in DataflowGraph.inputIdentifiers silently becomes an EXTERNAL input: no graph edge, no ordering — fresh runs fail TABLE_OR_VIEW_NOT_FOUND, re-runs silently read the previous run's materialized table",
      "sql/pipelines/.../graph/FlowAnalysis.scala → FlowAnalysis.readBatchInput / readExternalBatchInput; sql/pipelines/.../graph/Flow.scala → FlowFunctionResult.inputs",
      Coverage.Uncovered("directly measured 2026-06-13 (source-fails / middle-completes skip test): edge present fresh, ABSENT on re-run, identical on the Python CLI — an upstream bug, not our client"),
      List("TABLE_OR_VIEW_NOT_FOUND", "42P01"),
    ),
    Behavior(
      "DEF-4-views-not-inputs",
      Area.GraphDefaults,
      "inputIdentifiers = flows ++ tables: a view is NOT directly an input — it is reachable only through its (unqualified) implicit flow identifier",
      "sql/pipelines/.../graph/DataflowGraph.scala → DataflowGraph.inputIdentifiers",
      Coverage.Uncovered("our MV/TV reads go through SQL bodies, so we have never had to depend on this; it is the mechanism behind DEF-3 for view reads"),
    ),

    // ------------------------------------------------------------ materialization
    Behavior(
      "MAT-1-create",
      Area.Materialization,
      "a table that does not exist is created with the inferred-or-declared schema, merged properties and the declared transforms",
      "sql/pipelines/.../graph/DatasetManager.scala → DatasetManager.materializeTable / createTable",
      Coverage.Uncovered("exercised by hand in every live e2e run (real Iceberg/Delta tables verified); no automated assertion of the catalog outcome"),
    ),
    Behavior(
      "MAT-2-alter-only-on-rerun",
      Area.Materialization,
      "an existing table is only ALTERed, never dropped or replaced: additive column adds, type/nullability/comment updates, and setProperty for each declared property whose value differs",
      "sql/pipelines/.../graph/DatasetManager.scala → DatasetManager.evolveTable",
      Coverage.Uncovered("the provider re-assert bug lived exactly here — a set `format` makes the server re-apply `provider` on every re-run, which Delta rejects unconditionally; the encoder's fix (never send the default provider) is asserted, the server behavior is not"),
      List("TableChange.setProperty", "DELTA_CANNOT_CHANGE_PROVIDER"),
    ),
    Behavior(
      "MAT-3-props-never-removed",
      Area.Materialization,
      "a property dropped from the client's definition is NOT removed from the catalog table — properties are re-asserted, never pruned",
      "sql/pipelines/.../graph/DatasetManager.scala → DatasetManager.evolveTable (SPARK-57670)",
      Coverage.Uncovered("an authoring-surface hazard we have never tested: removing a table property from the pipeline has no effect"),
    ),
    Behavior(
      "MAT-4-schema-merge-vs-replace",
      Area.Materialization,
      "streaming table + non-full-refresh merges the existing schema with this run's; a materialized view (or any full refresh) takes this run's schema as-is, so a removed column becomes deleteColumn",
      "sql/pipelines/.../graph/DatasetManager.scala → DatasetManager.materializeTable (isTableIncrementallyUpdated); sql/pipelines/.../util/SchemaMergingUtils.scala → SchemaMergingUtils.mergeSchemas",
      Coverage.Uncovered("nothing asserts what a schema change does across two runs — the most likely next expensive surprise"),
      List("CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE"),
    ),
    Behavior(
      "MAT-5-partitioning",
      Area.Materialization,
      "partition/cluster columns cannot change on an existing table, and partitioning + clustering together is refused",
      "sql/pipelines/.../graph/DatasetManager.scala → DatasetManager.materializeTable",
      Coverage.NotApplicable("this client emits no partition or cluster transforms — DefineOutput carries name, type, provider and schema only"),
      List("CANNOT_UPDATE_PARTITION_COLUMNS", "SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED"),
    ),
    Behavior(
      "MAT-6-views-republished",
      Area.Materialization,
      "persisted views are re-published every run for the whole graph (CREATE OR REPLACE semantics), in dependency order, regardless of refresh selection",
      "sql/pipelines/.../graph/DatasetManager.scala → DatasetManager.materializeViews / materializeView",
      Coverage.Uncovered("our MVs are re-published on every run and we rely on it implicitly"),
    ),

    // --------------------------------------------------------------- full refresh
    Behavior(
      "FR-1-mv-truncate-every-run",
      Area.FullRefresh,
      "data is wiped iff the table exists and NOT (streaming && !fullRefresh) — so a materialized view is TRUNCATEd on EVERY run, by literal `TRUNCATE TABLE`, not a DSv2 overwrite",
      "sql/pipelines/.../graph/DatasetManager.scala → DatasetManager.materializeTable",
      Coverage.Uncovered("this is the Delta-vs-Iceberg re-run bug: the target provider must support TRUNCATE. Verified by hand on Iceberg 2026-06-11 (metadata-only snapshot swap); no spec asserts it"),
      List("TRUNCATE TABLE"),
    ),
    Behavior(
      "FR-2-checkpoint-roll",
      Area.FullRefresh,
      "full refresh rolls the streaming checkpoint into a new numbered sibling directory (the old one is left in place) and runs BEFORE materialization",
      "sql/pipelines/.../graph/State.scala → State.reset; sql/pipelines/.../graph/PipelineExecution.scala → PipelineExecution.startPipeline",
      Coverage.Uncovered("no client surface requests full refresh yet — StartRun carries only dry + storage — so the behavior is unreachable from here"),
    ),
    Behavior(
      "FR-3-selection-rules",
      Area.FullRefresh,
      "refresh/full-refresh selections are validated (no subset together with full_refresh_all, no overlap) and are NOT transitively expanded: unselected flows are EXCLUDED, which satisfies the topological gate",
      "sql/connect/server/.../connect/pipelines/PipelinesHandler.scala → PipelinesHandler.createTableFilters; sql/pipelines/.../graph/GraphFilter.scala → FlowsForTables",
      Coverage.Uncovered("we never send selections (every run is the default AllTables refresh); this row is the spec for a future sdpRefresh surface"),
      List("Datasets specified for refresh and full refresh cannot overlap:"),
    ),
    Behavior(
      "FR-4-reset-allowed",
      Area.FullRefresh,
      "pipelines.reset.allowed=false behaves differently per mode: an explicit full_refresh_selection errors, while full_refresh_all silently DEMOTES the table to a plain refresh",
      "sql/pipelines/.../graph/State.scala → State.findFlowsToReset; sql/pipelines/.../graph/DatasetManager.scala → DatasetManager.constructFullRefreshSet",
      Coverage.Uncovered("we neither set the property nor request full refresh; recorded because the silent demotion is a trap"),
      List("pipelines.reset.allowed", "TABLE_NOT_RESETTABLE"),
    ),

    // ----------------------------------------------------------------- once flows
    Behavior(
      "ONCE-1-server-rejects",
      Area.OnceFlows,
      "DefineFlow.once is rejected OUTRIGHT by the Connect server on master — the first check in defineFlow, before the graph lookup",
      "sql/connect/server/.../connect/pipelines/PipelinesHandler.scala → PipelinesHandler.defineFlow",
      Coverage.Uncovered("FINDING: still rejected at master HEAD, not just on 4.1.2 as our integration-spec comment says. Our `once` DSL surface validates and manifests offline but cannot be registered on ANY current server"),
      List("DEFINE_FLOW_ONCE_OPTION_NOT_SUPPORTED", "0A000"),
    ),
    Behavior(
      "ONCE-2-presence-not-value",
      Area.OnceFlows,
      "the server checks `hasOnce` — PRESENCE, not value — so explicitly sending once=false also fails; the field must be left unset",
      "sql/connect/server/.../connect/pipelines/PipelinesHandler.scala → PipelinesHandler.defineFlow",
      Coverage.Uncovered("our side of it is now pinned — PipelineProtoEncoderSpec asserts the field is ABSENT on every once=false flow, relation and AUTO CDC alike (S1). The SERVER's presence rule itself is still only source-read here"),
    ),
    Behavior(
      "ONCE-3-dormant-engine",
      Area.OnceFlows,
      "the engine-side once machinery (AppendOnceFlow, IDLE-on-no-data, skip-unless-full-refresh) exists but is unreachable through Connect: every registration path hardcodes once = false",
      "sql/pipelines/.../graph/Flow.scala → AppendOnceFlow; sql/pipelines/.../graph/TriggeredGraphExecution.scala → TriggeredGraphExecution.topologicalExecution",
      Coverage.NotApplicable("no client can exercise it on master; recorded so that flipping ONCE-1 upstream has a row waiting"),
    ),

    // ---------------------------------------------------------- external inputs
    Behavior(
      "EXT-1-external-resolution",
      Area.ExternalInputs,
      "a read that is not an in-graph dataset resolves as an ordinary catalog read on the fully-qualified name and is recorded ONLY in usedExternalInputs (no edge, no ordering)",
      "sql/pipelines/.../graph/FlowAnalysis.scala → FlowAnalysis.readExternalBatchInput / readExternalStreamInput",
      Coverage.Uncovered("D9 verified by hand: pushing an external-reading pipeline flipped the server error from managed-dataset rejection to a plain catalog lookup, proving the usedExternalInput path; no automated spec"),
    ),
    Behavior(
      "EXT-2-missing-external",
      Area.ExternalInputs,
      "an unresolvable read fails the run with the ordinary Spark analysis error, attributed to the flow and wrapped as an unresolved-pipeline failure",
      "sql/pipelines/.../graph/GraphValidations.scala → GraphValidations.validateSuccessfulFlowAnalysis; sql/pipelines/.../graph/PipelinesErrors.scala → UnresolvedDatasetException",
      Coverage.Covered(RegistrationIT, "a manifest with an undeclared upstream comes back as a typed ServerRejected carrying the server's own diagnostic"),
      List("TABLE_OR_VIEW_NOT_FOUND", "Dataset is defined in the pipeline but could not be resolved."),
    ),
    Behavior(
      "EXT-3-in-graph-virtual",
      Area.ExternalInputs,
      "an in-graph read is served by VirtualTableInput — an EMPTY DataFrame with the inferred schema — never the materialized table, so a flow can never see the previous run's rows of an in-graph dependency",
      "sql/pipelines/.../graph/elements.scala → VirtualTableInput.load; sql/pipelines/.../graph/CoreDataflowNodeProcessor.scala → CoreDataflowNodeProcessor.processNode",
      Coverage.Uncovered("a load-bearing authoring semantic (it is why DEF-3's silent re-run success is possible) that no spec or doc of ours states"),
    ),
    Behavior(
      "EXT-4-cycles",
      Area.ExternalInputs,
      "a circular in-graph dependency is detected during resolution and reported distinctly from an ordinary resolution failure",
      "sql/pipelines/.../graph/GraphValidations.scala → GraphValidations.validateSuccessfulFlowAnalysis (detectCycle)",
      Coverage.NotApplicable("our build-time validator refuses cycles offline (GraphValidation + the cyclic-pipeline scripted suite), so a cyclic graph never reaches a server"),
      List("Circular dependencies are not supported in a pipeline."),
    ),

    // -------------------------------------------------------------------- streaming
    Behavior(
      "STR-1-available-now",
      Area.Streaming,
      "every streaming flow runs with Trigger.AvailableNow() — there is NO continuous mode: the run processes all currently-available data and stops",
      "sql/pipelines/.../graph/TriggeredGraphExecution.scala → TriggeredGraphExecution.streamTrigger",
      Coverage.Uncovered("this is why sdpWatch re-triggers client-side; multi-cycle behavior verified by hand, asserted nowhere"),
      List("Trigger.AvailableNow()"),
    ),
    Behavior(
      "STR-2-checkpoint-resume",
      Area.Streaming,
      "a re-run resumes from the highest-numbered checkpoint directory under <storage>/_checkpoints/<catalog>/<db>/<dataset>/<flow> (created as 0 when absent)",
      "sql/pipelines/.../graph/FlowPlanner.scala → FlowPlanner.plan; sql/pipelines/.../graph/SystemMetadata.scala → SystemMetadata.getLatestCheckpointDir",
      Coverage.Uncovered("the incremental re-run property the whole watch loop rests on; hand-verified only"),
      List("checkpointLocation"),
    ),
    Behavior(
      "STR-3-termination",
      Area.Streaming,
      "a run ends when no flow is QUEUED, RUNNING or retry-pending; downstream of a permanent failure is SKIPPED, and the run is COMPLETED iff every state is a terminal non-failure",
      "sql/pipelines/.../graph/TriggeredGraphExecution.scala → TriggeredGraphExecution.topologicalExecution / getRunTerminationReason",
      Coverage.Uncovered("our drain races a timeout precisely because termination is the server's decision; the completion path runs in every live e2e but is asserted nowhere"),
    ),
    Behavior(
      "STR-4-flow-retries",
      Area.Streaming,
      "a non-fatal flow failure is retried with backoff (maxFlowRetryAttempts, default 2), so a client sees FAILED-then-RUNNING for the same flow before any terminal verdict",
      "sql/pipelines/.../graph/GraphExecution.scala → GraphExecution.determineFlowExecutionActionFromError; TriggeredGraphExecution.recordFailed",
      Coverage.Uncovered("RunProgress treats a FAILED event as terminal per flow; a retried flow therefore looks terminal and then moves again"),
      List("spark.sql.pipelines.maxFlowRetryAttempts"),
    ),

    // ---------------------------------------------------------------------- events
    Behavior(
      "EV-1-text-only",
      Area.Events,
      "the wire event carries ONLY timestamp + message: flow name, status, level and source location are dropped server-side, so progress must be recovered by parsing text",
      "sql/connect/server/.../connect/pipelines/PipelineEventSender.scala → PipelineEventSender.constructProtoEvent",
      Coverage.Covered(RunProgressS, "RunProgress.parse is the single place that does it, and is unit-tested against recorded server wording"),
    ),
    Behavior(
      "EV-2-flow-wording",
      Area.Events,
      "per-flow wording is inconsistent by design of its call sites (`Flow X is QUEUED.` vs `Flow 'X' has FAILED.`), so a client must match loosely",
      "sql/pipelines/.../logging/FlowProgressEventLogger.scala → recordQueued / recordStart / recordRunning / recordFailed / recordCompletion",
      Coverage.Covered(RunProgressS, "both shapes are fixtures, including the quoted-id failure message; an unrecognised message stays Unknown and lossless"),
      List("Flow <f> is QUEUED.", "Flow '<f>' has FAILED."),
    ),
    Behavior(
      "EV-3-unparsed-states",
      Area.Events,
      "master also emits SKIPPED, STOPPED and EXCLUDED flow messages, and emits PLANNING only for batch flows",
      "sql/pipelines/.../logging/FlowProgressEventLogger.scala → recordSkipped / recordSkippedOnUpStreamFailure / recordStop / recordExcluded",
      Coverage.Uncovered("FINDING: RunProgress.parse has no SKIPPED or STOPPED branch, so those messages classify as Unknown — lossless in the log, invisible to any future DAG view"),
      List("SKIPPED due to upstream failure(s).", "has STOPPED."),
    ),
    Behavior(
      "EV-4-no-terminal-failure-event",
      Area.Events,
      "a FAILED run emits NO terminal run event: RunProgress(FAILED) is intercepted and rethrown as the RPC error instead, and a CANCELED run becomes a generic exception",
      "sql/connect/server/.../connect/pipelines/PipelinesHandler.scala → PipelinesHandler.startRun (eventCallback) / throwRunFailure",
      Coverage.Covered(RegistrationIT, "the failure arrives as a typed ServerRejected from the RPC with the preceding events attached as context — exactly this shape"),
      List("PIPELINE_RUN_FAILED", "Pipeline run was canceled."),
    ),
    Behavior(
      "EV-5-dropped-events",
      Area.Events,
      "non-terminal events are SILENTLY DROPPED when the async send queue is full (capacity 1000); only RunProgress and terminal FlowProgress events are guaranteed",
      "sql/connect/server/.../connect/pipelines/PipelineEventSender.scala → PipelineEventSender.sendEvent / shouldEnqueueEvent",
      Coverage.Uncovered("our progress stream — and anything built on it — must tolerate gaps; nothing asserts that tolerance"),
      List("spark.sql.pipelines.event.queue.capacity"),
    ),
    Behavior(
      "EV-6-ordering",
      Area.Events,
      "the event sender is closed before StartRun returns, so every event precedes the final result or error on the response stream",
      "sql/connect/server/.../connect/pipelines/PipelineEventSender.scala → PipelineEventSender.shutdown",
      Coverage.Uncovered("the drain-then-verdict shape of RunHandle depends on this guarantee"),
    ),

    // --------------------------------------------------------------------- dry run
    Behavior(
      "DRY-1-validate-only",
      Area.DryRun,
      "dry = resolve + validate only: no State.reset, no materializeDatasets (no CREATE/ALTER/TRUNCATE, no view publish), no flow started",
      "sql/pipelines/.../graph/PipelineExecution.scala → PipelineExecution.dryRunPipeline (vs startPipeline)",
      Coverage.Covered(RegistrationIT, "every test in the suite registers with the default dry = true and the server validates a real graph"),
    ),
    Behavior(
      "DRY-2-indistinguishable",
      Area.DryRun,
      "a successful dry run emits the same single `Run is COMPLETED.` event as a real run — from the stream alone a client cannot tell them apart",
      "sql/pipelines/.../graph/PipelineExecution.scala → PipelineExecution.dryRunPipeline (constructTerminationEvent)",
      Coverage.Uncovered("our CLI distinguishes them only by what it asked for; worth knowing before trusting a log"),
      List("Run is COMPLETED."),
    ),
    Behavior(
      "DRY-3-validation-set",
      Area.DryRun,
      "both dry and real runs perform the same eight graph validations in a fixed order, first failure wins (flow analysis, user schemas, topological sort, multi-query tables, persisted-view sources, dataset-has-flow, resettability, flow streamingness)",
      "sql/pipelines/.../graph/DataflowGraph.scala → DataflowGraph.validate",
      Coverage.Uncovered("our offline validator deliberately duplicates three of them (cycles, dangling reads, schema) so the build fails in seconds; the other five are only ever seen from the server"),
      List("INVALID_FLOW_QUERY_TYPE.BATCH_RELATION_FOR_STREAMING_TABLE", "MATERIALIZED_VIEW_WITH_MULTIPLE_QUERIES"),
    ),
  )

  /** Rows grouped by area, with per-area and overall tallies — same shape and
    * conventions as [[SupportedCapabilities.report]] (generated numbers,
    * explicit "none", nothing hand-maintained). */
  def report: String =
    val header = List(
      "SDP behavioral conformance matrix (server behavior an SDP client depends on):",
      s"  Spark source anchors read at master HEAD 919f0808549; ${rows.size} behaviors enumerated.",
      "",
    )

    val areaBlocks = Area.values.toList.flatMap { area =>
      val inArea = rows.filter(_.area == area)
      if inArea.isEmpty then Nil
      else
        val covered = inArea.count(r => r.coverage.isInstanceOf[Coverage.Covered])
        val na      = inArea.count(r => r.coverage.isInstanceOf[Coverage.NotApplicable])
        val scope   = inArea.size - na
        val pct     = if scope == 0 then 100 else covered * 100 / scope
        val head    = f"  ${area.toString}%-15s ${covered}%2d / ${scope}%2d  ($pct%3d%%)  ${area.title}"
        val lines = inArea.map { r =>
          val detail = r.coverage match
            case Coverage.Covered(_, how)    => s"via: $how"
            case Coverage.Uncovered(note)    => if note.isEmpty then "" else s"gap: $note"
            case Coverage.NotApplicable(why) => s"n/a: $why"
          List(
            f"    ${r.id}%-32s [${r.coverage.label}]",
            s"      ${r.what}",
            if detail.isEmpty then "" else s"      $detail",
            s"      anchor: ${r.anchor}",
          ).filter(_.nonEmpty) ++
            (if r.matchable.isEmpty then Nil
             else List(s"      matches: ${r.matchable.mkString(", ")}"))
        }
        (head :: "" :: lines.flatten.toList) :+ ""
    }

    val inScope   = rows.filterNot(_.coverage.isInstanceOf[Coverage.NotApplicable])
    val covered   = inScope.count(_.coverage.isInstanceOf[Coverage.Covered])
    val uncovered = inScope.filter(_.coverage.isInstanceOf[Coverage.Uncovered])
    val naRows    = rows.filter(_.coverage.isInstanceOf[Coverage.NotApplicable])

    val totals = List(
      f"  TOTAL          ${covered}%2d / ${inScope.size}%2d  (${if inScope.isEmpty then 100 else covered * 100 / inScope.size}%3d%%)   not applicable: ${naRows.size}",
      s"  uncovered:     ${if uncovered.isEmpty then "none" else uncovered.map(_.id).mkString(", ")}",
      "",
      "  Uncovered rows are the point of this matrix, not a TODO list to hide:",
      "  the wire matrix was at 100% when D9's external-table gap shipped.",
    )

    (header ++ areaBlocks ++ totals).mkString("\n")

  /** `id -> status label`, the stable snapshot `BehaviorCoverageSpec` asserts.
    * A status that changes must change that assertion in the same commit —
    * coverage cannot drift silently in either direction. */
  def statusSnapshot: List[(String, String)] =
    rows.map(r => r.id -> r.coverage.label)

  /** The FQNs every `Covered` row claims. */
  def claimedSpecs: List[String] =
    rows.collect { case Behavior(_, _, _, _, Coverage.Covered(spec, _), _) => spec }.distinct
