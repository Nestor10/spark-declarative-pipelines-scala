package dev.sdp.plugin

import java.net.{URL, URLClassLoader}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}

import dev.sdp.connect.{AlgebraProtoEncoder, CatalogSeeder, PipelinesRegistration, PlanAnalysis}
import dev.sdp.connect.app.ValidationRendering
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
  * Fragment discovery is classload-eval (D10, replacing the earlier TASTy
  * scan): the DSL is now a plain runtime plan-builder, so the plugin loads the
  * pipeline object, calls `pipeline`, and renders each fragment to a STRING via
  * `dev.sdp.core.PipelineExport.encodeAll`. The string is the only thing that
  * crosses the classloader boundary — exactly the contract the TASTy embedding
  * used — so the child loader is fully isolated (no parent delegation for
  * user/sdp classes) and `loader.close()` runs in a finally. See
  * [[evalPipelineFragments]].
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
    val sdpConnectEndpoint = settingKey[String](
      "Spark Connect gRPC endpoint, e.g. sc://localhost:15002."
    )
    val sdpStorageRoot = settingKey[String](
      "Pipeline checkpoint/metadata root — absolute URI with scheme (file://, s3a://, ...)."
    )
    val sdpPushDryRun = settingKey[Boolean](
      "When true (default), sdpPush only validates server-side; no flows execute. (The dedicated " +
        "`sdpDryRun` task always runs dry regardless of this setting; `sdpRun` always runs for real.)"
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
      "Register the SDP graph with the remote PipelinesHandler; validate (dry) or run per sdpDryRun."
    )
    val sdpRun = taskKey[Unit](
      "Register and actually execute the SDP graph (dry = false) — materializes tables. One-shot run."
    )
    val sdpWatch = taskKey[Unit](
      "Re-trigger the pipeline every sdpWatchInterval seconds (Ctrl-C to stop). Each cycle is a " +
        "triggered run whose AvailableNow resumes from the checkpoint and picks up new data — the " +
        "server has no true continuous mode, so this is client-side periodic re-triggering."
    )
    val sdpWatchInterval = settingKey[Int](
      "Seconds between sdpWatch re-triggers. Default 30."
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
      "Version of the dev.sdp %% sdp-runtime-dsl AND sdp-connect libraries to inject. Defaults to " +
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
    // `sdp-runtime-dsl_3` / `sdp-connect_3`. Appending (+=) never clobbers the
    // consumer's other deps, and the setting key is the documented override
    // (or `dependencyOverrides`).
    //   - sdp-runtime-dsl: the authoring surface (`table`/`view`/`functions`).
    //   - sdp-connect: `SdpApp` (users extend it) + the Connect client. Needed
    //     on the consumer's classpath both so `object X extends SdpApp` compiles
    //     and so the plugin's child-loader eval can resolve SdpApp/PipelineExport.
    sdpRuntimeVersion := SdpBuildInfo.version,
    libraryDependencies += "dev.sdp" %% "sdp-runtime-dsl" % sdpRuntimeVersion.value,
    libraryDependencies += "dev.sdp" %% "sdp-connect"     % sdpRuntimeVersion.value,

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

    // Uncached on purpose: pushing to a remote server is a network effect,
    // not a pure function of the inputs (planning principle 3).
    sdpPush := Def.uncached {
      pushOrRun(
        log = streams.value.log,
        conv = fileConverter.value,
        endpoint = sdpConnectEndpoint.value,
        storage = sdpStorageRoot.value,
        manifestRef = sdpManifest.value,
        dry = sdpPushDryRun.value,
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
        endpoint = sdpConnectEndpoint.value,
        storage = sdpStorageRoot.value,
        manifestRef = sdpManifest.value,
        dry = false,
        timeoutSeconds = sdpRunTimeout.value,
      )
    },

    // Offline inner-loop verdict: evaluate + assemble + validate, print the
    // result, write NOTHING. Uncached so `~sdpValidate` re-runs every save.
    // Shares the classload-eval path with sdpManifest (the boundary is strings).
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
        endpoint = sdpConnectEndpoint.value,
        storage = sdpStorageRoot.value,
        manifestRef = sdpManifest.value,
        dry = true,
        timeoutSeconds = sdpRunTimeout.value,
      )
    },

    sdpWatch := Def.uncached {
      watchLoop(
        log = streams.value.log,
        conv = fileConverter.value,
        endpoint = sdpConnectEndpoint.value,
        storage = sdpStorageRoot.value,
        manifestRef = sdpManifest.value,
        intervalSeconds = sdpWatchInterval.value,
      )
    },

    sdpStorageRoot   := s"file:///tmp/sdp/${name.value}",
    sdpPushDryRun    := true,
    sdpRunTimeout    := 600,
    sdpWatchInterval := 30,

    sdpSchemasPackage := "sdp.schemas",
    sdpSchemasFile    := (Compile / scalaSource).value / "sdp" / "schemas" / "PipelineSchemas.scala",
    sdpCatalogTables  := Nil,
    sdpSeedStatements := Nil,

    // Uncached network effect: run the fixture SQL against the live server.
    sdpSeed := Def.uncached {
      val log        = streams.value.log
      val statements = sdpSeedStatements.value.toList
      if statements.isEmpty then log.info("sdp: sdpSeedStatements is empty — nothing to seed.")
      else
        val (host, port) = parseEndpoint(sdpConnectEndpoint.value)
        log.info(s"sdp: seeding ${statements.size} statement(s) on ${sdpConnectEndpoint.value}")
        SdpZioBridge.run(CatalogSeeder.run(host, port, statements)) match
          case Left(err) => sys.error(s"sdp: seeding failed — ${err.describe}")
          case Right(_)  => log.info(s"sdp: seeded ${statements.size} statement(s).")
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

      // (b) remote catalog tables, schemas from the live analyzer
      val catalogEntries =
        val tables = sdpCatalogTables.value.toList
        if tables.isEmpty then Nil
        else
          val (host, port) = parseEndpoint(sdpConnectEndpoint.value)
          tables.map { table =>
            SdpZioBridge.run(
              PlanAnalysis.analyzeSchema(
                host,
                port,
                AlgebraProtoEncoder.relation(
                  dev.sdp.core.algebra.Rel.NamedTable(table, streaming = false)
                ),
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
    * that replaced the TASTy scan.
    *
    * The cross-loader boundary is the fragment STRING (the same contract the
    * TASTy embedding used), so the child loader is built child-FIRST with the
    * PLATFORM loader as parent — i.e. NO delegation to the plugin's own loader
    * for user/sdp classes. That total isolation is safe precisely because
    * nothing but `String[]` crosses back: the child's `GraphFragment` and the
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
    // Child-first, parent = platform loader: JDK classes resolve, but user/sdp
    // classes do NOT delegate to the plugin loader — full isolation.
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

  /** Shared body for `sdpPush` (dry per setting) and `sdpRun`
    * (dry = false). Loads the manifest, registers + runs against the server,
    * and surfaces the run's events (validation diagnostics for a dry run;
    * flow progress + termination for a real one). */
  private def pushOrRun(
      log: sbt.util.Logger,
      conv: xsbti.FileConverter,
      endpoint: String,
      storage: String,
      manifestRef: HashedVirtualFileRef,
      dry: Boolean,
      timeoutSeconds: Int,
  ): Unit =
    val manifestPath = conv.toPath(manifestRef)
    val manifestText = new String(Files.readAllBytes(manifestPath), UTF_8)
    val manifest = dev.sdp.core.PipelineManifest
      .parse(manifestText)
      .fold(err => sys.error(s"sdp: unreadable manifest $manifestPath: $err"), identity)

    val (host, port) = parseEndpoint(endpoint)
    val verb         = if dry then "validating" else "running"
    log.info(s"sdp: $verb ${manifest.nodes.size} dataset(s) on $endpoint (dry=$dry, storage=$storage)")

    // Register (eager → graphId), then consume the run as a ZStream: each
    // event is logged live as it arrives (`.tap` — the same hook a future DAG
    // view renders from). Bound it by racing the drain against a timer that
    // force-closes the channel (`handle.cancel`): on a never-terminating run
    // the cancel unblocks the parked gRPC pull so the loser interrupts cleanly
    // (a plain `.timeout` would deadlock waiting on the parked `next()`).
    val effect = zio.ZIO.scoped {
      PipelinesRegistration.register(host, port, manifest, storage, dry).flatMap { handle =>
        val drain = handle.progress
          .tap(p => zio.ZIO.succeed(log.info(s"sdp:   • ${p.raw}")))
          .runDrain
          .as(true)
        val detach = zio.ZIO.sleep(zio.Duration.fromSeconds(timeoutSeconds.toLong)) *> handle.cancel.as(false)
        drain.raceFirst(detach).map(completed => (handle.graphId, completed))
      }
    }

    SdpZioBridge.run(effect) match
      case Left(err) =>
        sys.error(s"sdp: ${if dry then "validation" else "run"} failed — ${err.describe}")
      case Right((graphId, false)) =>
        log.warn(
          s"sdp: run still in progress after ${timeoutSeconds}s — detached. The server keeps " +
            "running; this is expected for an unbounded streaming source. Raise sdpRunTimeout " +
            s"or use a terminating source for a one-shot run. (graph id: $graphId)"
        )
      case Right((graphId, true)) =>
        val mode = if dry then "validated (dry run)" else "executed"
        log.info(s"sdp: pipeline $mode on the server; dataflow graph id: $graphId")

  /** Periodic re-trigger ("watch") — the server has no continuous execution, so
    * this re-issues a *triggered* run every `intervalSeconds`; each AvailableNow
    * cycle resumes from the checkpoint and processes newly-arrived data. Runs
    * until interrupted (Ctrl-C). A per-cycle failure stops the loop (`.repeat`
    * only continues on success). Each cycle registers a fresh graph — fine for
    * a dev loop, though long watches accumulate server-side graph metadata. */
  private def watchLoop(
      log: sbt.util.Logger,
      conv: xsbti.FileConverter,
      endpoint: String,
      storage: String,
      manifestRef: HashedVirtualFileRef,
      intervalSeconds: Int,
  ): Unit =
    val manifestPath = conv.toPath(manifestRef)
    val manifestText = new String(Files.readAllBytes(manifestPath), UTF_8)
    val manifest = dev.sdp.core.PipelineManifest
      .parse(manifestText)
      .fold(err => sys.error(s"sdp: unreadable manifest $manifestPath: $err"), identity)

    val (host, port) = parseEndpoint(endpoint)
    log.info(
      s"sdp: watching ${manifest.nodes.size} dataset(s) on $endpoint — re-triggering every " +
        s"${intervalSeconds}s (Ctrl-C to stop)"
    )

    val cycle = zio.ZIO.scoped {
      PipelinesRegistration.register(host, port, manifest, storage, dry = false).flatMap { handle =>
        handle.progress
          .tap(p => zio.ZIO.succeed(log.info(s"sdp:   • ${p.raw}")))
          .runDrain
          .as(handle.graphId)
      }
    }.tap(gid => zio.ZIO.succeed(log.info(s"sdp: cycle complete ($gid) — next in ${intervalSeconds}s")))

    // repeat forever on a fixed spacing; only an error (or Ctrl-C) ends it
    val loop = cycle.repeat(zio.Schedule.spaced(zio.Duration.fromSeconds(intervalSeconds.toLong))).unit

    SdpZioBridge.run(loop) match
      case Left(err) => sys.error(s"sdp: watch failed — ${err.describe}")
      case Right(_)  => () // unreachable under spaced(); Ctrl-C interrupts instead
}
