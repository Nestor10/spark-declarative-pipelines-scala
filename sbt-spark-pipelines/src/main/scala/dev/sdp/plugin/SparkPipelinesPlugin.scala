package dev.sdp.plugin

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

import dev.sdp.connect.{AlgebraProtoEncoder, CatalogSeeder, PipelinesRegistration, PlanAnalysis}
import dev.sdp.connect.app.ValidationRendering
import sbt.*
import sbt.Keys.*
import sbt.CacheImplicits.given
import xsbti.{HashedVirtualFileRef, VirtualFile}

/** The sbt 2.0 AutoPlugin for Spark Declarative Pipelines.
  *
  * `sdpManifest` is the heart: it scans the compiled `.tasty` output
  * for macro-embedded fragments (no classloading of project code — see
  * `TastyFragmentScanner`), validates the assembled graph on an isolated ZIO
  * runtime, and writes the canonical manifest. An invalid graph **fails the
  * build** with every accumulated error.
  *
  * The task is cached (`Def.cachedTask`): its inputs are the content-hashed
  * compiled products and classpath, its output is declared to the action
  * cache, so unchanged sources mean no rescan — and the manifest is
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
    val sdpDryRun = settingKey[Boolean](
      "When true (default), sdpPush only validates server-side; no flows execute."
    )
    val sdpRunTimeout = settingKey[Int](
      "Max seconds to wait for a run before detaching. Batch/terminating streaming runs " +
        "finish well under this; a never-terminating streaming source (e.g. rate) hits it and " +
        "detaches gracefully rather than wedging the build. Default 600."
    )
    val sdpManifest = taskKey[HashedVirtualFileRef](
      "Scan compiled .tasty for pipeline fragments, validate the DAG, write the manifest."
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
      "Version of the dev.sdp %% sdp-runtime-dsl library to inject. Defaults to the plugin's own " +
        "version (lockstep — the plugin and DSL share a TASTy/wire contract). Override only for " +
        "local testing or an emergency hotfix."
    )
  }

  import autoImport.*

  override lazy val projectSettings: Seq[Setting[?]] = Seq(
    sdpConnectEndpoint := "sc://localhost:15002",

    // Version lockstep: the consumer writes ONE version (the addSbtPlugin
    // line); the matching runtime DSL is injected automatically. `%%` adds
    // the Scala 3 `_3` suffix, matching the published `sdp-runtime-dsl_3`.
    // Appending (+=) never clobbers the consumer's other deps, and the
    // setting key is the documented override (or `dependencyOverrides`).
    sdpRuntimeVersion := SdpBuildInfo.version,
    libraryDependencies += "dev.sdp" %% "sdp-runtime-dsl" % sdpRuntimeVersion.value,

    sdpManifest := (Def.cachedTask {
      val log  = streams.value.log
      val conv = fileConverter.value

      // Cache inputs: content-hashed compiled products (the .tasty we scan)
      // and the dependency classpath (resolution context). Both are
      // Seq[Attributed[HashedVirtualFileRef]] in sbt 2 — any change to
      // compiled output changes the hash and invalidates this task.
      val products  = (Compile / exportedProducts).value
      val classpath = (Compile / fullClasspath).value
      val targetDir = (Compile / target).value

      val targets  = products.toList.map(entry => conv.toPath(entry.data))
      val depPaths = classpath.toList.map(entry => conv.toPath(entry.data))

      val fragments = TastyFragmentScanner.scan(targets, depPaths)
      log.info(s"sdp: discovered ${fragments.size} pipeline fragment(s)")

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
        dry = sdpDryRun.value,
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
    sdpDryRun        := true,
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

      // (a) the pipeline's own datasets, shapes inferred from compiled flows
      val _        = (Compile / compile).value
      val classDir = (Compile / classDirectory).value.toPath
      val depPaths = (Compile / dependencyClasspath).value.toList.map(e => conv.toPath(e.data))
      val fragments = TastyFragmentScanner.scan(List(classDir), depPaths)

      val ownEntries = SdpZioBridge.assemble(fragments) match
        case Left(errors) =>
          sys.error(s"sdp: cannot import schemas from an invalid pipeline:\n${ValidationRendering.renderErrors(errors.toList)}")
        case Right(manifest) =>
          import dev.sdp.core.algebra.SchemaCheck
          val order    = manifest.toGraph.topologicalSort.getOrElse(Nil)
          val byTarget = manifest.flows.groupBy(_.target).view.mapValues(_.map(f => (f.name, f.relation))).toMap
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
