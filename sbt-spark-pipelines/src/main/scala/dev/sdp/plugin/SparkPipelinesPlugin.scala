package dev.sdp.plugin

import java.net.{URL, URLClassLoader}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}

import dev.sdp.connect.{AlgebraProtoEncoder, CatalogSeeder, PipelinesRegistration, PlanAnalysis}
import dev.sdp.connect.app.{SdpCommands, ValidationRendering}
import dev.sdp.core.GraphFragment
import sbt.*
import sbt.Keys.*
import sbt.CacheImplicits.given
import xsbti.{HashedVirtualFileRef, VirtualFile}

/** The sbt 2.0 AutoPlugin for Spark Declarative Pipelines.
  *
  * `sdpManifest` is the heart: it evaluates the user's pipeline object
  * (`sdpPipelineClass`) in a child classloader over the project's runtime
  * classpath, validates the assembled graph on an isolated ZIO runtime, and
  * writes the canonical manifest. An invalid graph **fails the build** with
  * every accumulated error.
  *
  * Fragment discovery is classload-eval (D10): the DSL is a runtime
  * plan-builder, so the plugin loads the pipeline object, calls `pipeline`, and
  * renders each fragment to a STRING via
  * `dev.sdp.core.PipelineExport.encodeAll`. The string is the only thing that
  * crosses the classloader boundary, so the eval loader can be fully isolated —
  * its parent is the PLATFORM loader, which holds no user or sdp classes, so
  * nothing delegates back to the plugin's own loader — and `loader.close()`
  * runs in a finally. See [[evalPipelineFragments]].
  *
  * The task is cached (`Def.cachedTask`): its inputs are the content-hashed
  * compiled products and classpath, its output is declared to the action
  * cache, so unchanged sources mean no re-eval — and the manifest is
  * restorable from local/remote cache.
  */
object SparkPipelinesPlugin extends AutoPlugin {

  override def trigger: PluginTrigger = noTrigger
  override def requires: Plugins      = plugins.JvmPlugin

  object autoImport {
    /** The typed environment descriptor, auto-imported so `build.sbt` can write
      * `SdpTarget(...)` / `SdpTarget.userScopedDev(...)` with no import. */
    type SdpTarget = dev.sdp.plugin.SdpTarget
    val SdpTarget: dev.sdp.plugin.SdpTarget.type = dev.sdp.plugin.SdpTarget

    val sdpTargets = settingKey[Map[String, SdpTarget]](
      "Named deployment environments (dev/stage/prod) as typed SdpTarget values — the descriptor " +
        "for `sdpRunOn`/`sdpDryRunOn`/`sdpSeedOn <target>`. A target varies WHERE a pipeline runs " +
        "(endpoint, catalog/database, storage, TLS/token), never WHAT it is: the manifest is " +
        "target-independent, so dev and prod run identical bytes. Default empty. Validated when a " +
        "target is USED, not at build load."
    )
    val sdpConnectEndpoint = settingKey[String](
      "Spark Connect gRPC endpoint, e.g. sc://localhost:15002."
    )
    val sdpConnectUseTls = settingKey[Boolean](
      "Speak TLS to sdpConnectEndpoint instead of plaintext. Default false — sc://localhost is the " +
        "dev container. Set true for a managed endpoint (together with sdpConnectToken)."
    )
    val sdpConnectToken = settingKey[String](
      "Bearer token attached to every Spark Connect call (Authorization: Bearer …). \"\" (default) = " +
        "anonymous; defaults to the SDP_CONNECT_TOKEN environment variable so the secret need not " +
        "live in build.sbt. Never logged."
    )
    val sdpConnectDeadline = settingKey[Int](
      "Per-RPC deadline in seconds for the registration/seed/analyze calls (default 60) so a wedged " +
        "server cannot hang the build. The run stream is bounded by sdpRunTimeout instead."
    )
    val sdpStorageRoot = settingKey[String](
      "Pipeline checkpoint/metadata root — absolute URI with scheme (file://, s3a://, ...)."
    )
    val sdpDefaultCatalog = settingKey[String](
      "Graph default catalog sent in CreateDataflowGraph (proto field 1). \"\" (default) = omit. " +
        "The official Python client always sends these; omission can mis-qualify reads on named " +
        "V2 catalogs (dependency edges silently vanish)."
    )
    val sdpDefaultDatabase = settingKey[String](
      "Graph default database (proto field 2) — the dev/prod switch: unqualified dataset names " +
        "land here (dev_eric locally, the real schema in prod). \"\" (default) = omit."
    )
    val sdpPushDryRun = settingKey[Boolean](
      "DEPRECATED (removed in 0.4) along with `sdpPush`: when true (default), sdpPush only " +
        "validates server-side; no flows execute. Use `sdpDryRun` (always dry) or `sdpRun` " +
        "(always real) instead — neither reads this setting."
    )
    val sdpVersionCheck = settingKey[Boolean](
      "When true (the default), sdpDryRun/sdpRun/sdpWatch ask the server which Spark it " +
        "is before registering anything, and refuse a pipeline that uses constructs newer than " +
        "the server (proto3 drops unknown fields, so an old server would otherwise fail " +
        "confusingly). Set false only for a fork whose version string we read wrongly."
    )
    val sdpRunTimeout = settingKey[Int](
      "Max seconds to wait for a run before detaching. Batch/terminating streaming runs " +
        "finish well under this; a never-terminating streaming source (e.g. rate) hits it and " +
        "detaches gracefully rather than wedging the build. Default 600."
    )
    val sdpPipelineClass = settingKey[String](
      "Fully-qualified name of the user's pipeline object — an `object X extends SdpApp`, or any " +
        "object exposing `def pipeline: List[GraphFragment]`. The plugin loads it in a child " +
        "classloader over the project's runtime classpath, calls `pipeline`, and assembles the graph."
    )
    val sdpManifest = taskKey[HashedVirtualFileRef](
      "Evaluate the pipeline object, validate the DAG, write the manifest."
    )
    val sdpValidate = taskKey[Unit](
      "Assemble + validate the pipeline graph and print the verdict — no file output. The offline " +
        "inner-loop target for `~sdpValidate`."
    )
    val sdpDryRun = taskKey[Unit](
      "Register the graph server-side in validate-only mode (dry run) — the target for `~sdpDryRun`."
    )
    val sdpPush = taskKey[Unit](
      "DEPRECATED (removed in 0.4) — a third spelling of a pair that already exists. Use " +
        "`sdpDryRun` (always dry) or `sdpRun` (always real), or `sdpDryRunOn <target>` / " +
        "`sdpRunOn <target>` for a named environment. Still delegates per sdpPushDryRun for now."
    )
    val sdpRun = taskKey[Unit](
      "Register and actually execute the SDP graph (dry = false) — materializes tables. One-shot run."
    )
    val sdpFullRefresh = taskKey[Unit](
      "Register and execute the graph with StartRun.full_refresh_all — the \"rebuild everything\" " +
        "button. The SERVER does the work: it rolls each streaming flow's checkpoint into a new " +
        "numbered sibling directory and then wipes the targets, so every table recomputes from its " +
        "sources. Destructive and never cached. Note `pipelines.reset.allowed=false` on a table " +
        "silently DEMOTES it to an ordinary refresh — see docs/plugin.md."
    )
    val sdpFullRefreshOn = inputKey[Unit](
      "`sdpFullRefreshOn <target>` — sdpFullRefresh against a named sdpTargets environment " +
        "(tab-completes the names). Destructive in whichever environment you name: `prod` deserves " +
        "the same care here as any other production-destructive action."
    )
    val sdpWatch = taskKey[Unit](
      "Re-trigger the pipeline every sdpWatchInterval seconds (Ctrl-C to stop). Each cycle is a " +
        "triggered run whose AvailableNow resumes from the checkpoint and picks up new data — the " +
        "server has no true continuous mode, so this is client-side periodic re-triggering."
    )
    val sdpWatchInterval = settingKey[Int](
      "Seconds between sdpWatch re-triggers. Default 30."
    )
    val sdpRunOn = inputKey[Unit](
      "`sdpRunOn <target>` — sdpRun against a named sdpTargets environment. The target supplies " +
        "ONLY the connection (endpoint, catalog/database, storage, TLS/token); the manifest is the " +
        "same bytes every target runs."
    )
    val sdpDryRunOn = inputKey[Unit](
      "`sdpDryRunOn <target>` — sdpDryRun against a named sdpTargets environment. Full server-side " +
        "Catalyst validation against that environment's real catalog, zero execution: the pre-merge gate."
    )
    val sdpSeedOn = inputKey[Unit](
      "`sdpSeedOn <target>` — run sdpSeedStatements against a named sdpTargets environment."
    )
    val sdpSchemasPackage = settingKey[String](
      "Package for generated named-tuple schema aliases."
    )
    val sdpSchemasFile = settingKey[File](
      "Output file for generated schema aliases (checked-in source, like slick-codegen)."
    )
    val sdpCatalogTables = settingKey[Seq[String]](
      "Remote catalog tables to import schemas for (via AnalyzePlan against sdpConnectEndpoint)."
    )
    val sdpImportSchemas = taskKey[Unit](
      "Generate named-tuple schema aliases from the pipeline's inferred shapes and the remote catalog."
    )
    val sdpSeedStatements = settingKey[Seq[String]](
      "SQL statements (DDL/DML) run by `sdpSeed` against sdpConnectEndpoint — a local fixture to " +
        "create + populate the source/catalog tables an `externalTable` reads, so a full run resolves them."
    )
    val sdpSeed = taskKey[Unit](
      "Run sdpSeedStatements against the Spark Connect server (local catalog-fixture seeding)."
    )
    val sdpRuntimeVersion = settingKey[String](
      "Version of the sdp library to inject. Defaults to " +
        "the plugin's own version (lockstep — the plugin, DSL and Connect client share the fragment " +
        "string + wire contract). Override only for local testing or an emergency hotfix."
    )
  }

  import autoImport.*

  override lazy val projectSettings: Seq[Setting[?]] = Seq(
    sdpConnectEndpoint := "sc://localhost:15002",

    // Version lockstep: the consumer writes ONE version (the addSbtPlugin
    // line); the matching runtime DSL AND Connect client are injected
    // automatically. `%%` adds the Scala 3 `_3` suffix, matching the published
    // `sdp_3`. Appending (+=) never clobbers the
    // consumer's other deps, and the setting key is the documented override
    // (or `dependencyOverrides`).
    //   - sdp: the authoring surface (`dev.sdp.dsl`), `SdpApp` (users extend
    //     it), and the Connect client — one artifact (2026-06-11 collapse).
    //     Needed on the consumer's classpath both so `object X extends SdpApp`
    //     compiles and so the child-loader eval can resolve SdpApp/PipelineExport.
    sdpRuntimeVersion := SdpBuildInfo.version,
    libraryDependencies += SdpBuildInfo.organization %% "sdp" % sdpRuntimeVersion.value,

    sdpPipelineClass := "",

    sdpManifest := (Def.cachedTask {
      val log  = streams.value.log
      val conv = fileConverter.value

      // Cache inputs: the full RUNTIME classpath, content-hashed. This is the
      // exact set of jars/dirs the child loader evaluates over (it includes the
      // project's own compiled products via exportedProducts), so any change to
      // the pipeline code OR its deps changes the hash and invalidates this
      // task. Seq[Attributed[HashedVirtualFileRef]] in sbt 2.
      val classpath = (Runtime / fullClasspath).value
      val targetDir = (Compile / target).value
      val fqn       = sdpPipelineClass.value

      val cpPaths   = classpath.toList.map(entry => conv.toPath(entry.data))

      val fragments = evalPipelineFragments(fqn, cpPaths, log)
      log.info(s"sdp: evaluated ${fragments.size} pipeline fragment(s) from $fqn")

      SdpZioBridge.assemble(fragments) match
        case Left(errors) =>
          sys.error(ValidationRendering.invalidGraphMessage(errors.toList))

        case Right(manifest) =>
          val out = targetDir.toPath.resolve("sdp").resolve("pipeline.sdpm")
          Files.createDirectories(out.getParent)
          Files.write(out, manifest.render.getBytes(UTF_8))
          log.info(s"sdp: wrote manifest (${manifest.nodes.size} dataset(s)) to $out")

          // Register the side effect with the action cache: on a cache hit
          // the manifest is materialized from the CAS instead of recomputed.
          val vf: VirtualFile = conv.toVirtualFile(out)
          Def.declareOutput(vf)
          (vf: HashedVirtualFileRef)
    }).value,

    // DEPRECATED — scheduled for removal in 0.4.
    //
    // `sdpPush` + `sdpPushDryRun` is a third spelling of a pair that already
    // exists in two better ones: `sdpDryRun`/`sdpRun` say what they do in their
    // name, and `sdpDryRunOn`/`sdpRunOn <target>` add the environment. A task
    // whose meaning depends on a separate boolean setting is exactly the shape
    // that makes a build script ambiguous to read — "did this materialize
    // tables?" should never require looking somewhere else. It still works, and
    // still honours `sdpPushDryRun`, so nothing breaks today; it warns once per
    // invocation and names its replacement.
    //
    // Uncached on purpose: pushing to a remote server is a network effect,
    // not a pure function of the inputs (planning principle 3).
    sdpPush := Def.uncached {
      val log = streams.value.log
      val dry = sdpPushDryRun.value
      log.warn(
        s"sdp: `sdpPush` is deprecated and will be removed in 0.4. Use `sdpDryRun` (always dry) " +
          s"or `sdpRun` (always real) — or `sdpDryRunOn <target>` / `sdpRunOn <target>` for a " +
          s"named environment. Delegating to ${if dry then "sdpDryRun" else "sdpRun"} " +
          s"(sdpPushDryRun := $dry)."
      )
      pushOrRun(
        log = log,
        conv = fileConverter.value,
        conn = TargetResolution.base(baseConnection.value),
        manifestRef = sdpManifest.value,
        dry = dry,
        timeoutSeconds = sdpRunTimeout.value,
      )
    },

    // The real-execution counterpart: dry = false unconditionally, so it
    // materializes tables. A dedicated task (rather than `set sdpDryRun :=
    // false; sdpPush`) sidesteps the sbt-2.0 thin-client `set` parse
    // quirk and reads as intent. The run is one-shot (TriggeredGraphExecution)
    // so it terminates; flow-progress events are surfaced.
    sdpRun := Def.uncached {
      pushOrRun(
        log = streams.value.log,
        conv = fileConverter.value,
        conn = TargetResolution.base(baseConnection.value),
        manifestRef = sdpManifest.value,
        dry = false,
        timeoutSeconds = sdpRunTimeout.value,
      )
    },

    // The "rebuild everything" button (Databricks calls it Full Refresh). Same
    // body as `sdpRun` with one flag flipped — because it IS a run, in a
    // different MODE: the client sends StartRun.full_refresh_all and the SERVER
    // does the destructive work (State.reset rolls each streaming checkpoint
    // into a new numbered sibling BEFORE materialization; materialization then
    // wipes the targets). Nothing is deleted from here, which is why there is no
    // filesystem code in this plugin and no "are you sure?" prompt: the blast
    // radius is whatever the named connection points at, exactly as for sdpRun.
    //
    // Uncached, like every network task — a full refresh must be reachable
    // twice in a row without the action cache deciding the second one is a
    // no-op (which is precisely what an author wants when the first one
    // demoted silently; see docs/plugin.md on pipelines.reset.allowed).
    sdpFullRefresh := Def.uncached {
      pushOrRun(
        log = streams.value.log,
        conv = fileConverter.value,
        conn = TargetResolution.base(baseConnection.value),
        manifestRef = sdpManifest.value,
        dry = false,
        timeoutSeconds = sdpRunTimeout.value,
        fullRefresh = true,
      )
    },

    // Offline inner-loop verdict: evaluate + assemble + validate, print the
    // result, write NOTHING. Uncached so `~sdpValidate` re-runs every save.
    // Shares the classload-eval path with sdpManifest (the boundary is strings).
    //
    // P1, STRUCTURALLY: neither this task nor `sdpManifest` mentions
    // `sdpTargets`, `sdpConnectEndpoint` or any other connection setting — and
    // must never start. A target varies WHERE a pipeline runs, never WHAT it
    // is, so the manifest cannot depend on one; that is what makes "the hash
    // you validated in dev is the hash prod runs" a build property instead of
    // a promise, and it also keeps the cached task's key honest.
    sdpValidate := Def.uncached {
      val log = streams.value.log
      val conv = fileConverter.value
      val cpPaths = (Runtime / fullClasspath).value.toList.map(entry => conv.toPath(entry.data))
      val fragments = evalPipelineFragments(sdpPipelineClass.value, cpPaths, log)
      SdpZioBridge.assemble(fragments) match
        case Left(errors)   => sys.error(ValidationRendering.invalidGraphMessage(errors.toList))
        case Right(manifest) =>
          log.info(s"sdp: pipeline valid — ${manifest.nodes.size} dataset(s), ${manifest.flows.size} flow(s).")
    },

    // Dry run: register the graph server-side in validate-only mode. Target for
    // `~sdpDryRun`. Reuses the (cached) manifest and the shared push path.
    sdpDryRun := Def.uncached {
      pushOrRun(
        log = streams.value.log,
        conv = fileConverter.value,
        conn = TargetResolution.base(baseConnection.value),
        manifestRef = sdpManifest.value,
        dry = true,
        timeoutSeconds = sdpRunTimeout.value,
      )
    },

    // NOT `sdpManifest.value`: a watch that resolved the manifest once would
    // keep re-triggering the pipeline the author had when they pressed enter,
    // silently ignoring every edit since. It takes the same two inputs the
    // classload-eval needs and re-runs it EVERY cycle instead — see watchLoop.
    sdpWatch := Def.uncached {
      val conv = fileConverter.value
      watchLoop(
        log = streams.value.log,
        fqn = sdpPipelineClass.value,
        classpath = (Runtime / fullClasspath).value.toList.map(entry => conv.toPath(entry.data)),
        conn = TargetResolution.base(baseConnection.value),
        intervalSeconds = sdpWatchInterval.value,
        timeoutSeconds = sdpRunTimeout.value,
      )
    },

    // ---------------------------------------------------------------------
    // Target-addressable tasks (E2). Each one resolves `<target>` through
    // `sdpTargets` and then calls EXACTLY the body its untargeted twin calls —
    // the target's only contribution is the `ResolvedConnection`. Nothing here
    // touches `sdpManifest`'s inputs, so `sdpRunOn dev` and `sdpRunOn prod`
    // register identical manifest bytes; that is the promotion property, and
    // the scripted `targets` suite asserts it by hash.
    //
    // Input tasks are uncached by construction (`Def.inputTask` never builds a
    // cached task), which is what these are: network effects parameterised by
    // a command-line argument. Reading the environment for a target's
    // `tokenEnv` therefore happens here, at task time — not in a setting, and
    // never inside a cached task body.
    // ---------------------------------------------------------------------
    sdpRunOn := {
      val requested = targetNameParser.parsed
      pushOrRun(
        log = streams.value.log,
        conv = fileConverter.value,
        conn = resolveTarget(baseConnection.value, sdpTargets.value, requested, "sdpRunOn"),
        manifestRef = sdpManifest.value,
        dry = false,
        timeoutSeconds = sdpRunTimeout.value,
      )
    },

    sdpDryRunOn := {
      val requested = targetNameParser.parsed
      pushOrRun(
        log = streams.value.log,
        conv = fileConverter.value,
        conn = resolveTarget(baseConnection.value, sdpTargets.value, requested, "sdpDryRunOn"),
        manifestRef = sdpManifest.value,
        dry = true,
        timeoutSeconds = sdpRunTimeout.value,
      )
    },

    // A full refresh is a run MODE, not a connection property — so the target
    // contributes exactly what it contributes to `sdpRunOn` (endpoint, catalog/
    // database, storage, TLS/token) and nothing more. `SdpTarget` gains no
    // field, `ResolvedConnection` gains no field, and the manifest is untouched:
    // `sdpFullRefreshOn prod` registers the same bytes `sdpRunOn prod` does.
    sdpFullRefreshOn := {
      val requested = targetNameParser.parsed
      pushOrRun(
        log = streams.value.log,
        conv = fileConverter.value,
        conn = resolveTarget(baseConnection.value, sdpTargets.value, requested, "sdpFullRefreshOn"),
        manifestRef = sdpManifest.value,
        dry = false,
        timeoutSeconds = sdpRunTimeout.value,
        fullRefresh = true,
      )
    },

    sdpSeedOn := {
      val requested = targetNameParser.parsed
      seed(
        log = streams.value.log,
        conn = resolveTarget(baseConnection.value, sdpTargets.value, requested, "sdpSeedOn"),
        statements = sdpSeedStatements.value.toList,
      )
    },

    // Plaintext + anonymous by default: the inner loop talks to a local
    // container. The token default reads the environment ONCE, in a setting —
    // never inside a cached task body (that would break cache stability).
    sdpConnectUseTls   := false,
    sdpConnectToken    := sys.env.getOrElse("SDP_CONNECT_TOKEN", ""),
    sdpConnectDeadline := 60,
    // The handshake is ON by default: the inner loop is exactly where someone
    // points a 4.2-capable client at the 4.1 container they already had running.
    sdpVersionCheck := true,

    // No environments declared by default: the flat settings above ARE the
    // single implicit target (the local container), so a build that never
    // deploys anywhere never has to learn about targets.
    sdpTargets := Map.empty,

    sdpStorageRoot   := s"file:///tmp/sdp/${name.value}",
    sdpPushDryRun    := true,
    sdpDefaultCatalog  := "",
    sdpDefaultDatabase := "",
    sdpRunTimeout    := 600,
    sdpWatchInterval := 30,

    sdpSchemasPackage := "sdp.schemas",
    sdpSchemasFile    := (Compile / scalaSource).value / "sdp" / "schemas" / "PipelineSchemas.scala",
    sdpCatalogTables  := Nil,
    sdpSeedStatements := Nil,

    // Uncached network effect: run the fixture SQL against the live server.
    sdpSeed := Def.uncached {
      seed(
        log = streams.value.log,
        conn = TargetResolution.base(baseConnection.value),
        statements = sdpSeedStatements.value.toList,
      )
    },

    // Writes into checked-in sources by design (the codegen-import pattern:
    // schema changes become reviewable diffs) — uncached, explicitly invoked.
    sdpImportSchemas := Def.uncached {
      val log  = streams.value.log
      val conv = fileConverter.value

      // (a) the pipeline's own datasets, shapes inferred from compiled flows.
      // Same classload-eval as sdpManifest: evaluate the pipeline object over
      // the runtime classpath, get fragment strings back across the boundary.
      val cpPaths   = (Runtime / fullClasspath).value.toList.map(e => conv.toPath(e.data))
      val fragments = evalPipelineFragments(sdpPipelineClass.value, cpPaths, log)

      val ownEntries = SdpZioBridge.assemble(fragments) match
        case Left(errors) =>
          sys.error(s"sdp: cannot import schemas from an invalid pipeline:\n${ValidationRendering.renderErrors(errors.toList)}")
        case Right(manifest) =>
          import dev.sdp.core.algebra.SchemaCheck
          val order    = manifest.toGraph.topologicalSort.getOrElse(Nil)
          // Only WriteRelation flows carry a relation to infer a shape from;
          // AUTO CDC flows contribute a gradual-Unknown target shape.
          val relFlows = manifest.flows.collect {
            case f @ dev.sdp.core.Flow(_, _, _: dev.sdp.core.FlowDetails.WriteRelation, _) => f
          }
          val byTarget = relFlows.groupBy(_.target).view.mapValues(_.map(f => (f.name, f.relation))).toMap
          val (_, shapes) = SchemaCheck.propagate(order, byTarget)
          shapes.toList.collect { case (dataset, SchemaCheck.Shape.Known(cols)) =>
            dev.sdp.core.algebra.SchemaCodegen.Entry(dataset, cols, "pipeline-inferred")
          }

      // (b) remote catalog tables, schemas from the live analyzer. Codegen is a
      // build-time authoring aid, not a deployment, so it reads the flat
      // settings only — there is no `sdpImportSchemasOn`.
      val conn = TargetResolution.base(baseConnection.value)
      val catalogEntries =
        val tables = sdpCatalogTables.value.toList
        if tables.isEmpty then Nil
        else
          val (host, port) = parseEndpoint(conn.endpoint)
          tables.map { table =>
            SdpZioBridge.run(
              PlanAnalysis.analyzeSchema(
                host,
                port,
                AlgebraProtoEncoder.relation(
                  dev.sdp.core.algebra.Rel.NamedTable(table, streaming = false)
                ),
                conn.transport,
              )
            ) match
              case Left(err) => sys.error(s"sdp: catalog import failed for '$table' — ${err.describe}")
              case Right(fields) =>
                dev.sdp.core.algebra.SchemaCodegen.Entry(
                  table,
                  fields.map(f => f.name -> kindToColType(f.kind)),
                  s"catalog: $table",
                )
          }

      val entries = ownEntries ++ catalogEntries
      val out     = sdpSchemasFile.value.toPath
      Files.createDirectories(out.getParent)
      Files.write(
        out,
        dev.sdp.core.algebra.SchemaCodegen.render(sdpSchemasPackage.value, entries).getBytes(UTF_8),
      )
      log.info(s"sdp: wrote ${entries.size} schema alias(es) to $out")
    },
  )

  /** The flat connection settings as ONE value — the single place `build.sbt`'s
    * `sdpConnectEndpoint`/`sdpStorageRoot`/`sdpDefault*`/`sdpConnect*`/
    * `sdpVersionCheck` are read.
    *
    * Every network task derives its [[ResolvedConnection]] from this, either
    * directly (`TargetResolution.base`) or folded with a named [[SdpTarget]]
    * (`TargetResolution.select`), so there is exactly one resolution rule and
    * exactly one task body per operation. Reading the settings in one
    * `Def.setting` also keeps the eight `.value`s from being re-typed at every
    * call site, which is how they drifted before.
    */
  private lazy val baseConnection: Def.Initialize[BaseConnection] = Def.setting(
    BaseConnection(
      endpoint = sdpConnectEndpoint.value,
      storageRoot = sdpStorageRoot.value,
      defaultCatalog = sdpDefaultCatalog.value,
      defaultDatabase = sdpDefaultDatabase.value,
      useTls = sdpConnectUseTls.value,
      token = sdpConnectToken.value,
      deadlineSeconds = sdpConnectDeadline.value,
      versionCheck = sdpVersionCheck.value,
    )
  )

  /** The `<target>` argument of `sdpRunOn` / `sdpDryRunOn` / `sdpSeedOn`, with
    * TAB-completion over the declared `sdpTargets` keys.
    *
    * A setting-dependent parser (`Initialize[State => Parser[…]]`), because the
    * completions are the build's own target names. It deliberately accepts an
    * unknown name rather than failing to parse: an unparsed argument gets sbt's
    * generic syntax error, whereas letting the task resolve it produces our
    * one-line "unknown target 'x'. Available targets: …" — the message is the
    * feature.
    */
  private lazy val targetNameParser: Def.Initialize[sbt.State => sbt.internal.util.complete.Parser[String]] =
    Def.setting { (_: sbt.State) =>
      import sbt.internal.util.complete.DefaultParsers.*
      val names = sdpTargets.value.keys.toList.sorted
      Space ~> token(StringBasic.examples(names*), "<target>")
    }

  /** Resolve `<target>` against `sdpTargets`, failing the task with the whole
    * rendered message when it cannot (unknown name, empty map, invalid target,
    * missing token variable). Environment access happens HERE — task time. */
  private def resolveTarget(
      base: BaseConnection,
      targets: Map[String, SdpTarget],
      requested: String,
      taskName: String,
  ): ResolvedConnection =
    TargetResolution
      .select(base, targets, requested, sys.env.get, taskName)
      .fold(message => sys.error(message), identity)

  /** Proto DataType kind names (lower-cased KindCase) → ColType. */
  private def kindToColType(kind: String): dev.sdp.core.algebra.ColType =
    import dev.sdp.core.algebra.ColType
    kind match
      case "boolean"   => ColType.Bool
      case "integer"   => ColType.I32
      case "long"      => ColType.I64
      case "double"    => ColType.F64
      case "string"    => ColType.Str
      case "timestamp" => ColType.Timestamp
      case "date"      => ColType.Date
      case _           => ColType.Unknown

  /** Evaluate the user's pipeline object in an isolated child classloader and
    * recover its fragments as plain `GraphFragment`s — the D10 classload-eval
    * introduced by D10.
    *
    * The cross-loader boundary is the fragment STRING, so the eval loader is
    * built with the PLATFORM loader as its parent: ordinary parent-first
    * delegation, but the parent holds only JDK classes, so user and sdp classes
    * resolve from the project classpath and NEVER delegate to the plugin's own
    * loader. (It is isolation by a bare parent, not a child-first/inverted
    * delegation order.) That total isolation is safe precisely because nothing
    * but `String[]` crosses back: the evaluated `GraphFragment` and the
    * plugin's `GraphFragment` are different `Class`es and never meet.
    *
    * Reflection call-chain (all inside the child loader):
    *   1. `loader.loadClass("<fqn>$")`            — the user object's module class
    *   2. `.getField("MODULE$").get(null)`        — the singleton instance
    *   3. `.getMethod("pipeline").invoke(module)` — the `List[GraphFragment]` value
    *   4. `loader.loadClass("dev.sdp.core.PipelineExport$").getField("MODULE$")`
    *   5. `.getMethod("encodeAll", classOf[Object]).invoke(export, pipelineValue)`
    *        → `Array[String]` (crosses the boundary as bootstrap-loaded Strings)
    *   6. plugin-side: `GraphFragment.parse` each string into the plugin's own
    *      `GraphFragment`, feeding the existing assembly/validation flow.
    *
    * `loader.close()` runs in a finally so a warm sbt server never leaks loaders.
    * A malformed boundary string is a defect (bug in PipelineExport/codec).
    */
  private def evalPipelineFragments(
      fqn: String,
      classpath: List[Path],
      log: sbt.util.Logger,
  ): List[GraphFragment] =
    if fqn.trim.isEmpty then
      sys.error(
        "sdp: sdpPipelineClass is not set. Point it at your pipeline object, e.g. " +
          "`sdpPipelineClass := \"com.example.MyPipeline\"` (an `object … extends SdpApp`, or any " +
          "object with `def pipeline: List[GraphFragment]`)."
      )

    val urls: Array[URL] = classpath.map(_.toUri.toURL).toArray
    // Parent = the PLATFORM loader: JDK classes resolve through it, while
    // user/sdp classes can only come from `urls` — so nothing delegates to the
    // plugin's loader. Isolation by a bare parent, not by inverted delegation.
    val loader = new URLClassLoader(urls, ClassLoader.getPlatformClassLoader)
    try
      val moduleClass = loader.loadClass(fqn + "$")
      val module      = moduleClass.getField("MODULE$").get(null)
      val pipelineVal = moduleClass.getMethod("pipeline").invoke(module)

      val exportClass = loader.loadClass("dev.sdp.core.PipelineExport$")
      val exportMod   = exportClass.getField("MODULE$").get(null)
      val encoded     = exportClass
        .getMethod("encodeAll", classOf[Object])
        .invoke(exportMod, pipelineVal)
        .asInstanceOf[Array[String]]

      log.debug(s"sdp: classload-eval of $fqn produced ${encoded.length} fragment string(s)")

      encoded.toList.map { line =>
        GraphFragment.parse(line) match
          case Right(frag) => frag
          case Left(bad) =>
            throw new IllegalStateException(
              s"sdp: PipelineExport produced an undecodable fragment line: '$bad'. " +
                "This is a bug in the sdp string codec (plugin and runtime versions out of lockstep?)."
            )
      }
    catch
      case e: ClassNotFoundException =>
        sys.error(
          s"sdp: could not load pipeline object '$fqn' from the project classpath. " +
            s"Check sdpPipelineClass and that the object compiles. (${e.getMessage})"
        )
      case e: NoSuchMethodException =>
        sys.error(
          s"sdp: '$fqn' has no `def pipeline` returning List[GraphFragment]. " +
            s"Extend SdpApp or expose `def pipeline`. (${e.getMessage})"
        )
      case e: java.lang.reflect.InvocationTargetException =>
        val cause = Option(e.getCause).getOrElse(e)
        sys.error(s"sdp: evaluating '$fqn'.pipeline failed — ${cause.getClass.getName}: ${cause.getMessage}")
    finally loader.close()

  /** `sc://host:port` → (host, port), with a readable failure. */
  private def parseEndpoint(endpoint: String): (String, Int) =
    endpoint match
      case s"sc://$host:$port" if port.toIntOption.isDefined => (host, port.toInt)
      case other =>
        sys.error(s"sdp: sdpConnectEndpoint must look like sc://host:port, got '$other'")

  /** Read + parse the manifest a network task is about to register. */
  private def readManifest(
      conv: xsbti.FileConverter,
      manifestRef: HashedVirtualFileRef,
  ): dev.sdp.core.PipelineManifest =
    val manifestPath = conv.toPath(manifestRef)
    val manifestText = new String(Files.readAllBytes(manifestPath), UTF_8)
    dev.sdp.core.PipelineManifest
      .parse(manifestText)
      .fold(err => sys.error(s"sdp: unreadable manifest $manifestPath: $err"), identity)

  /** THE shared body for `sdpPush` (dry per setting), `sdpDryRun`, `sdpRun` and
    * their target-addressed `*On` forms. Loads the manifest, registers + runs
    * against the server described by `conn`, and surfaces the run's events
    * (validation diagnostics for a dry run; flow progress + termination for a
    * real one).
    *
    * Everything environment-specific arrives as ONE [[ResolvedConnection]] — a
    * named target and the flat settings differ only in how that value was
    * produced (`TargetResolution.select` vs `.base`), never in what happens
    * next. That is deliberate: `sdpRunOn dev` must be the same code path as
    * `sdpRun`, or "the same bytes ran in both" stops meaning anything.
    */
  private def pushOrRun(
      log: sbt.util.Logger,
      conv: xsbti.FileConverter,
      conn: ResolvedConnection,
      manifestRef: HashedVirtualFileRef,
      dry: Boolean,
      timeoutSeconds: Int,
      fullRefresh: Boolean = false,
  ): Unit =
    // The run-mode rule lives in the library (SdpCommands.checkRunMode), not
    // here: `sdp run --dry --full-refresh` from a user's uber jar and a dry
    // full refresh through the plugin must refuse in the SAME words, or the two
    // surfaces have quietly grown different semantics. No task below can
    // actually produce the combination — it is defence in depth on the shared
    // body, which is the only place that could ever grow it.
    SdpCommands.checkRunMode(dry, fullRefresh).left.foreach(reason => sys.error(s"sdp: $reason"))

    val manifest = readManifest(conv, manifestRef)

    val verb = if dry then "validating" else if fullRefresh then "FULL-REFRESHING" else "running"
    val full = if fullRefresh then ", full-refresh=true" else ""
    log.info(
      s"sdp: $verb ${manifest.nodes.size} dataset(s) on ${conn.endpoint} " +
        s"(${conn.origin}, dry=$dry$full, storage=${conn.storageRoot})"
    )

    SdpZioBridge.run(registerAndDrain(log, conn, manifest, dry, timeoutSeconds, fullRefresh)) match
      case Left(err) =>
        // Name the endpoint AND its provenance: with several targets in play,
        // "which environment did this fail against?" is the first question, and
        // a transport failure otherwise answers it only in the log above.
        val what = if dry then "validation" else if fullRefresh then "full refresh" else "run"
        sys.error(
          s"sdp: $what failed on ${conn.endpoint} (${conn.origin}) — ${err.describe}"
        )
      case Right((graphId, false)) =>
        log.warn(
          s"sdp: run still in progress after ${timeoutSeconds}s — detached. The server keeps " +
            "running; this is expected for an unbounded streaming source. Raise sdpRunTimeout " +
            s"or use a terminating source for a one-shot run. (graph id: $graphId)"
        )
      case Right((graphId, true)) =>
        val mode =
          if dry then "validated (dry run)"
          else if fullRefresh then "fully refreshed"
          else "executed"
        log.info(s"sdp: pipeline $mode on the server; dataflow graph id: $graphId")

  /** THE registration-and-drain effect, shared by [[pushOrRun]] and every
    * [[watchLoop]] cycle (E's one-body rule, extended rather than re-forked).
    *
    * Register (eager → graphId), then consume the run as a ZStream: each event
    * is logged live as it arrives (`.tap` — the same hook a future DAG view
    * renders from). Bound it by racing the drain against a timer that
    * force-closes the channel (`handle.cancel`): on a never-terminating run the
    * cancel unblocks the parked gRPC pull so the loser interrupts cleanly (a
    * plain `.timeout` would deadlock waiting on the parked `next()`).
    *
    * @return (graph id, whether the run finished before the timeout)
    */
  private def registerAndDrain(
      log: sbt.util.Logger,
      conn: ResolvedConnection,
      manifest: dev.sdp.core.PipelineManifest,
      dry: Boolean,
      timeoutSeconds: Int,
      fullRefresh: Boolean = false,
  ): zio.IO[PipelinesRegistration.RegistrationError, (String, Boolean)] =
    val (host, port) = parseEndpoint(conn.endpoint)
    zio.ZIO.scoped {
      PipelinesRegistration
        .register(
          host,
          port,
          manifest,
          conn.storageRoot,
          dry,
          fullRefresh,
          defaultCatalog = conn.defaultCatalog,
          defaultDatabase = conn.defaultDatabase,
          transport = conn.transport,
          versionCheck = conn.versionCheck,
        )
        .flatMap { handle =>
          val drain = handle.progress
            .tap(p => zio.ZIO.succeed(log.info(s"sdp:   • ${p.raw}")))
            .runDrain
            .as(true)
          val detach =
            zio.ZIO.sleep(zio.Duration.fromSeconds(timeoutSeconds.toLong)) *> handle.cancel.as(false)
          drain.raceFirst(detach).map(completed => (handle.graphId, completed))
        }
    }

  /** Evaluate the pipeline object and assemble its manifest, as an EFFECT.
    *
    * Deliberately NOT `SdpZioBridge.assemble`: a watch cycle is already running
    * inside an isolated runtime, and nesting a second one inside a fiber would
    * block one of its threads on a whole other scheduler. The error channel is
    * the rendered message, because at this point there is nothing left to
    * decide — the author reads it and fixes their code.
    */
  private def evaluateManifest(
      fqn: String,
      classpath: List[Path],
      log: sbt.util.Logger,
  ): zio.IO[String, dev.sdp.core.PipelineManifest] =
    zio.ZIO
      // Blocking: classload-eval reads jars off disk and runs user code.
      .attemptBlocking(evalPipelineFragments(fqn, classpath, log))
      .mapError(t => Option(t.getMessage).getOrElse(t.toString))
      .flatMap { fragments =>
        dev.sdp.app.ManifestAssembly
          .assemble(fragments)
          .provide(dev.sdp.app.ManifestAssembly.live, dev.sdp.app.GraphValidation.live)
          .mapError(errors => ValidationRendering.invalidGraphMessage(errors.toList))
      }

  /** The watch loop's SHAPE, independent of what a cycle does: evaluate, run,
    * space, repeat. Separated so the property that matters — that `evaluate`
    * runs once per cycle and not once per watch — is unit-testable without a
    * classpath or a server (`SdpWatchSpec`).
    *
    * `evaluate` is a by-name-ish ZIO value and therefore re-run on every
    * repetition; that single fact IS the fix. `.repeat` only continues on
    * success, so any cycle failure (a broken edit as much as a rejected
    * registration) ends the watch with its message.
    */
  private[plugin] def watchEffect[A](
      evaluate: zio.IO[String, A],
      cycle: A => zio.IO[String, Unit],
      intervalSeconds: Int,
  ): zio.IO[String, Unit] =
    evaluate
      .flatMap(cycle)
      .repeat(zio.Schedule.spaced(zio.Duration.fromSeconds(intervalSeconds.toLong)))
      .unit

  /** Periodic re-trigger ("watch") — the server has no continuous execution, so
    * this re-issues a *triggered* run every `intervalSeconds`; each AvailableNow
    * cycle resumes from the checkpoint and processes newly-arrived data. Runs
    * until interrupted (Ctrl-C). Each cycle registers a fresh graph — fine for
    * a dev loop, though long watches accumulate server-side graph metadata.
    *
    * Two things a watch owes the author, added in P3.2:
    *
    *   - **it re-evaluates the pipeline every cycle.** The manifest used to be
    *     resolved once, at task start, so every edit made during a long watch
    *     was silently ignored and the author watched stale code run. Now each
    *     cycle re-runs the classload-eval (loader per cycle, `close()`d — the
    *     existing hygiene) and re-assembles, so the next trigger is the code
    *     that is on disk. Costs ~2.6s per cycle, the measured inner loop.
    *   - **a wedged cycle cannot wedge the watch.** Cycles share `pushOrRun`'s
    *     drain-vs-detach race, so an unbounded streaming source detaches after
    *     `sdpRunTimeout` and the watch moves on instead of blocking forever.
    */
  private def watchLoop(
      log: sbt.util.Logger,
      fqn: String,
      classpath: List[Path],
      conn: ResolvedConnection,
      intervalSeconds: Int,
      timeoutSeconds: Int,
  ): Unit =
    log.info(
      s"sdp: watching ${conn.endpoint} (${conn.origin}) — re-evaluating $fqn and re-triggering " +
        s"every ${intervalSeconds}s (Ctrl-C to stop)"
    )

    val runCycle: dev.sdp.core.PipelineManifest => zio.IO[String, Unit] = manifest =>
      zio.ZIO.succeed(
        log.info(s"sdp: cycle — ${manifest.nodes.size} dataset(s), ${manifest.flows.size} flow(s)")
      ) *>
        registerAndDrain(log, conn, manifest, dry = false, timeoutSeconds)
          .mapError(err => s"run failed on ${conn.endpoint} (${conn.origin}) — ${err.describe}")
          .map {
            case (graphId, true) =>
              log.info(s"sdp: cycle complete ($graphId) — next in ${intervalSeconds}s")
            case (graphId, false) =>
              log.warn(
                s"sdp: cycle still running after ${timeoutSeconds}s — detached ($graphId); the " +
                  s"server keeps going. Next cycle in ${intervalSeconds}s."
              )
          }

    SdpZioBridge.run(
      watchEffect(evaluateManifest(fqn, classpath, log), runCycle, intervalSeconds)
    ) match
      case Left(message) => sys.error(s"sdp: watch stopped — $message")
      case Right(_)      => () // unreachable under spaced(); Ctrl-C interrupts instead

  /** THE shared body for `sdpSeed` and `sdpSeedOn` — fixture SQL against the
    * server described by `conn`. Same one-value-in shape as [[pushOrRun]]. */
  private def seed(
      log: sbt.util.Logger,
      conn: ResolvedConnection,
      statements: List[String],
  ): Unit =
    if statements.isEmpty then log.info("sdp: sdpSeedStatements is empty — nothing to seed.")
    else
      val (host, port) = parseEndpoint(conn.endpoint)
      log.info(
        s"sdp: seeding ${statements.size} statement(s) on ${conn.endpoint} (${conn.origin})"
      )
      SdpZioBridge.run(CatalogSeeder.run(host, port, statements, conn.transport)) match
        case Left(err) => sys.error(s"sdp: seeding failed on ${conn.endpoint} (${conn.origin}) — ${err.describe}")
        case Right(_)  => log.info(s"sdp: seeded ${statements.size} statement(s).")
}
