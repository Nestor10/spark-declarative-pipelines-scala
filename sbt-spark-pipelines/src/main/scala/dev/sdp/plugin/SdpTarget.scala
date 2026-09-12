package dev.sdp.plugin

import java.net.URI

/** One deployment **environment** — a place a pipeline can be registered and
  * run. The typed descriptor behind `sdpTargets` in `build.sbt`.
  *
  * This is the dbt/DAB "targets" ergonomic on this project's terms, and it obeys
  * two principles that the rest of the design leans on:
  *
  *   - **P1 — parameters vary WHERE, never WHAT.** Every field here is
  *     *connection-shaped*: which server, which catalog/database unqualified
  *     names land in, where checkpoints live, how to authenticate. Nothing here
  *     can change the *graph*: there is deliberately no hook for
  *     env-conditional dataset construction. The manifest is therefore one
  *     deterministic artifact per pipeline and the bytes you validated in dev
  *     are the bytes prod runs (see `docs/environments.md`).
  *   - **P2 — YAML is output, not source of truth.** The descriptor is Scala in
  *     `build.sbt`. YAML shows up only as an *emitted* artifact (the future
  *     `sdpExportArgo`) and as 12factor env on the runner.
  *
  * Every field maps 1:1 onto something the plugin already had as a flat setting
  * (`sdpConnectEndpoint`, `sdpDefaultCatalog`, …) and, through those, onto the
  * `SDP_*` environment variables `SdpApp` reads in production — so a target is
  * not a parallel configuration model, it is a *named bundle* of the existing
  * one. `TargetResolution` is where the two meet.
  *
  * @param connectEndpoint
  *   Spark Connect endpoint, `sc://host:port`. The one required field.
  * @param defaultCatalog
  *   graph default catalog (`CreateDataflowGraph` field 1); `None` = omit, which
  *   falls back to `sdpDefaultCatalog`.
  * @param defaultDatabase
  *   graph default database (field 2) — where unqualified managed datasets land.
  *   `None` = fall back to `sdpDefaultDatabase`. This is the dev/prod switch.
  * @param storageRoot
  *   checkpoint/metadata root, an absolute URI with a scheme. `None` = fall back
  *   to `sdpStorageRoot`.
  * @param useTls
  *   speak TLS to this endpoint. Not an `Option`: TLS is a property *of the
  *   endpoint*, so a target always answers the question (default `false`,
  *   matching a local container).
  * @param tokenEnv
  *   the **NAME of an environment variable** holding the bearer token — never
  *   the token itself. There is deliberately no `token: String` field: a
  *   literal secret must be impossible to express in `build.sbt` (which is
  *   committed, printed by `show`, and shipped in scripted sandboxes). The
  *   variable is read at *task* runtime, and the value never reaches a log or an
  *   error message (`TransportConfig.toString` redacts it). `None` = fall back
  *   to `sdpConnectToken`, which itself defaults to `SDP_CONNECT_TOKEN`.
  * @param deadlineSeconds
  *   per-RPC deadline override; `None` = fall back to `sdpConnectDeadline`.
  * @param versionCheck
  *   server-version handshake override; `None` = fall back to
  *   `sdpVersionCheck`. Per-target because a target *is* a server: the prod
  *   endpoint may be a fork whose version string this client reads wrongly
  *   while the dev container is stock.
  * @param schedule
  *   **Argo-reserved, dormant today.** Cron expression for the emitted
  *   `CronWorkflow`. Nothing in the current plugin reads it; `sdpExportArgo`
  *   will (see ROADMAP "Argo outer loop"). It lives here now so the target
  *   model does not have to change shape when that lands.
  * @param namespace
  *   **Argo-reserved, dormant today.** Kubernetes namespace to emit into.
  * @param runnerImage
  *   **Argo-reserved, dormant today.** Container image of the `SdpApp` runner
  *   the emitted workflow invokes.
  */
final case class SdpTarget(
    connectEndpoint: String,
    defaultCatalog: Option[String] = None,
    defaultDatabase: Option[String] = None,
    storageRoot: Option[String] = None,
    useTls: Boolean = false,
    tokenEnv: Option[String] = None,
    deadlineSeconds: Option[Int] = None,
    versionCheck: Option[Boolean] = None,
    // --- consumed by the future sdpExportArgo; inert in every current task ---
    schedule: Option[String] = None,
    namespace: Option[String] = None,
    runnerImage: Option[String] = None,
):

  /** Pure validation: every problem with this target, as readable sentences.
    * Empty means valid.
    *
    * Validation is applied when a target is **used** (by an `*On` task), not at
    * build load: an unused prod target with a typo must not break an offline
    * `sdpValidate`, and load-time failure would make `sdpTargets` hostile to
    * keep in a shared `build.sbt`.
    */
  def problems: List[String] =
    SdpTarget.endpointProblem(connectEndpoint).toList
      ++ storageRoot.flatMap(SdpTarget.storageRootProblem).toList
      ++ SdpTarget.blankProblem("defaultCatalog", defaultCatalog).toList
      ++ SdpTarget.blankProblem("defaultDatabase", defaultDatabase).toList
      ++ SdpTarget.envVarNameProblem(tokenEnv).toList
      ++ deadlineSeconds
        .filter(_ <= 0)
        .map(d => s"deadlineSeconds must be a positive number of seconds, got $d")
        .toList

object SdpTarget:

  /** A dev target that is **collision-free by default**: the database is
    * `dev_<user>`, derived from the JVM's `user.name`, so two engineers pointed
    * at the same server never write the same tables and nobody has to configure
    * anything (E3).
    *
    * Reading `sys.props` here is deliberate and allowed: this runs at
    * *setting* construction (build load), not inside a `Def.cachedTask` body —
    * that is the case the cache-stability rule prohibits, because a cached
    * task's inputs must be visible to the cache. The `*On` tasks that consume a
    * target are uncached network tasks by construction.
    *
    * **The Nessie alternative (recommended on the demo stack):** instead of
    * renaming the *schema*, pin a *catalog* to a branch
    * (`spark.sql.catalog.warehouse_dev.ref = dev-<user>`) and point
    * `defaultCatalog` at it. Identical table names, an isolated timeline, and
    * `MERGE BRANCH dev-<user> INTO main` promotes every table the pipeline
    * touched atomically — the data pull-request. See `docs/environments.md`.
    *
    * @param connectEndpoint
    *   the dev server, typically the local container.
    * @param catalog
    *   graph default catalog; `""` (the default) omits it.
    */
  def userScopedDev(connectEndpoint: String, catalog: String = ""): SdpTarget =
    SdpTarget(
      connectEndpoint = connectEndpoint,
      defaultCatalog = Some(catalog.trim).filter(_.nonEmpty),
      defaultDatabase = Some(devDatabaseFor(sys.props.getOrElse("user.name", ""))),
    )

  /** `"Eric Smith"` → `"dev_eric_smith"`. Lower-cased, every non-alphanumeric
    * run mapped to `_`, so the result is a legal unquoted identifier in every
    * catalog we target. A blank/unknown user degrades to `dev_user` rather than
    * to the bare prefix. */
  def devDatabaseFor(userName: String): String =
    val sanitized = userName.trim.toLowerCase
      .map(ch => if ch.isLetterOrDigit then ch else '_')
      .dropWhile(_ == '_')
      .reverse
      .dropWhile(_ == '_')
      .reverse
    if sanitized.isEmpty then "dev_user" else s"dev_$sanitized"

  /** `sc://host:port` or a sentence saying what is wrong with it. */
  private[plugin] def endpointProblem(endpoint: String): Option[String] =
    endpoint match
      case s"sc://$rest" =>
        rest.split(":", -1).toList match
          case host :: port :: Nil =>
            if host.trim.isEmpty then Some(s"connectEndpoint has an empty host: '$endpoint'")
            else
              port.toIntOption match
                case Some(p) if p > 0 && p <= 65535 => None
                case _ =>
                  Some(s"connectEndpoint port must be 1-65535, got '$port' in '$endpoint'")
          case _ =>
            Some(s"connectEndpoint must look like sc://host:port, got '$endpoint'")
      case other =>
        Some(s"connectEndpoint must look like sc://host:port, got '$other'")

  /** The storage root must be an absolute URI WITH a scheme — a bare
    * `/tmp/sdp` reaches the server as a relative path against *its* working
    * directory, which is the classic "checkpoints vanished" report. */
  private[plugin] def storageRootProblem(root: String): Option[String] =
    if root.trim.isEmpty then Some("storageRoot is empty — omit it (None) to inherit sdpStorageRoot")
    else
      try
        val uri = new URI(root)
        if Option(uri.getScheme).isEmpty then
          Some(
            s"storageRoot must be an absolute URI with a scheme (file://, s3a://, …), got '$root'"
          )
        else if !uri.isAbsolute then Some(s"storageRoot must be absolute, got '$root'")
        else None
      catch
        case e: java.net.URISyntaxException =>
          Some(s"storageRoot is not a valid URI ('$root'): ${e.getReason}")

  private def blankProblem(field: String, value: Option[String]): Option[String] =
    value.filter(_.trim.isEmpty).map(_ => s"$field is present but blank — omit it (None) instead")

  private def envVarNameProblem(name: Option[String]): Option[String] =
    name.flatMap { n =>
      if n.trim.isEmpty then Some("tokenEnv is present but blank — omit it (None) instead")
      else if n.exists(_.isWhitespace) then
        Some(s"tokenEnv must be the NAME of an environment variable, got '$n'")
      else None
    }
