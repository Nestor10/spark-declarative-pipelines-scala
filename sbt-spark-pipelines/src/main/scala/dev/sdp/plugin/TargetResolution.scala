package dev.sdp.plugin

import dev.sdp.connect.TransportConfig

/** The flat per-build connection settings, exactly as `build.sbt` spells them —
  * `sdpConnectEndpoint`, `sdpStorageRoot`, `sdpDefaultCatalog`/`Database`,
  * `sdpConnectUseTls`/`Token`/`Deadline`, `sdpVersionCheck`.
  *
  * Strings (not `Option`s) because that is what the settings are: `""` means
  * "omit", the convention those keys already document.
  */
private[plugin] final case class BaseConnection(
    endpoint: String,
    storageRoot: String,
    defaultCatalog: String,
    defaultDatabase: String,
    useTls: Boolean,
    token: String,
    deadlineSeconds: Int,
    versionCheck: Boolean,
)

/** Everything a network task needs, after the base settings and (optionally) a
  * named [[SdpTarget]] have been folded together. One value, so every task
  * shares one body: see `SparkPipelinesPlugin.pushOrRun` / `watchLoop` / `seed`.
  *
  * @param origin
  *   human-readable provenance for the log line — `"build settings"` or
  *   `"target 'dev'"`. Never contains a secret.
  */
private[plugin] final case class ResolvedConnection(
    endpoint: String,
    storageRoot: String,
    defaultCatalog: Option[String],
    defaultDatabase: Option[String],
    transport: TransportConfig,
    versionCheck: Boolean,
    origin: String,
):
  /** Redacts, like `TransportConfig` — safe to log. */
  override def toString: String =
    s"ResolvedConnection(endpoint=$endpoint, storageRoot=$storageRoot, " +
      s"defaultCatalog=${defaultCatalog.getOrElse("<omitted>")}, " +
      s"defaultDatabase=${defaultDatabase.getOrElse("<omitted>")}, " +
      s"transport=$transport, versionCheck=$versionCheck, origin=$origin)"

/** Where the named-target model meets the flat settings model.
  *
  * Pure and total: every failure is a `Left` carrying the whole message the task
  * should print. No `sys.error`, no logging, no environment access — the
  * environment arrives as the `env` function so the resolution is unit-testable
  * (`TargetResolutionSpec`).
  *
  * **P1 lives here, structurally.** The only things this can produce are
  * connection-shaped: endpoint, storage root, graph defaults, transport. There
  * is no path from a target to the manifest, and `sdpManifest`/`sdpValidate`
  * never mention `sdpTargets` at all — that is what makes byte-identical
  * promotion a property of the build rather than a promise in a doc.
  */
private[plugin] object TargetResolution:

  /** No target named: the flat settings, as the existing tasks always used them. */
  def base(b: BaseConnection): ResolvedConnection =
    ResolvedConnection(
      endpoint = b.endpoint,
      storageRoot = b.storageRoot,
      defaultCatalog = Some(b.defaultCatalog).map(_.trim).filter(_.nonEmpty),
      defaultDatabase = Some(b.defaultDatabase).map(_.trim).filter(_.nonEmpty),
      transport = transportConfig(b.useTls, b.token, b.deadlineSeconds),
      versionCheck = b.versionCheck,
      origin = "build settings",
    )

  /** Fold one named target over the base settings.
    *
    * Override rules, all of them "the target decides what it states":
    *   - `connectEndpoint` — always the target's (it is the point of a target).
    *   - `defaultCatalog` / `defaultDatabase` / `storageRoot` — the target's
    *     when present, otherwise the base setting.
    *   - `useTls` — always the target's: TLS is a property of the endpoint, and
    *     a target that moved the endpoint must not inherit the old answer.
    *   - `tokenEnv` — when present, the bearer token is read from that
    *     environment variable *now*; absent, the base `sdpConnectToken` (which
    *     itself defaults to `SDP_CONNECT_TOKEN`) carries over.
    *   - `deadlineSeconds` / `versionCheck` — the target's when stated.
    */
  def resolve(
      b: BaseConnection,
      name: String,
      target: SdpTarget,
      env: String => Option[String],
  ): Either[String, ResolvedConnection] =
    val problems = target.problems
    if problems.nonEmpty then
      Left(
        s"sdp: target '$name' is invalid:\n" + problems.map(p => s"  - $p").mkString("\n")
      )
    else
      resolveToken(name, target, b.token, env).map { token =>
        ResolvedConnection(
          endpoint = target.connectEndpoint,
          storageRoot = target.storageRoot.getOrElse(b.storageRoot),
          defaultCatalog =
            target.defaultCatalog.orElse(Some(b.defaultCatalog).map(_.trim).filter(_.nonEmpty)),
          defaultDatabase =
            target.defaultDatabase.orElse(Some(b.defaultDatabase).map(_.trim).filter(_.nonEmpty)),
          transport = transportConfig(
            target.useTls,
            token,
            target.deadlineSeconds.getOrElse(b.deadlineSeconds),
          ),
          versionCheck = target.versionCheck.getOrElse(b.versionCheck),
          origin = s"target '$name'",
        )
      }

  /** Look a target up by name and resolve it, with the messages an `*On` task
    * should print when it cannot.
    *
    * @param taskName
    *   the invoking task (`"sdpRunOn"`), so the "no targets declared" message
    *   can show the snippet that fixes it.
    */
  def select(
      b: BaseConnection,
      targets: Map[String, SdpTarget],
      requested: String,
      env: String => Option[String],
      taskName: String,
  ): Either[String, ResolvedConnection] =
    val wanted = requested.trim
    if targets.isEmpty then Left(noTargetsMessage(taskName))
    else if wanted.isEmpty then
      Left(s"sdp: $taskName needs a target name. Available targets: ${available(targets)}.")
    else
      targets.get(wanted) match
        case Some(target) => resolve(b, wanted, target, env)
        case None =>
          Left(s"sdp: unknown target '$wanted'. Available targets: ${available(targets)}.")

  private def available(targets: Map[String, SdpTarget]): String =
    targets.keys.toList.sorted.mkString(", ")

  private def noTargetsMessage(taskName: String): String =
    s"""sdp: `$taskName` needs `sdpTargets`, which is empty. Declare your environments in build.sbt:
       |
       |  sdpTargets := Map(
       |    "dev"  -> SdpTarget.userScopedDev("sc://localhost:15002", catalog = "warehouse"),
       |    "prod" -> SdpTarget(
       |      connectEndpoint = "sc://spark-connect.prod.svc:15002",
       |      defaultCatalog  = Some("warehouse"),
       |      defaultDatabase = Some("analytics"),
       |      useTls          = true,
       |      tokenEnv        = Some("SDP_PROD_TOKEN"),
       |    ),
       |  )
       |
       |Then: $taskName dev   (see docs/environments.md)""".stripMargin

  /** A target's `tokenEnv` names an environment variable; this reads it. The
    * failure message names the VARIABLE and never the value — a token must not
    * be reconstructible from a build log. */
  private def resolveToken(
      name: String,
      target: SdpTarget,
      baseToken: String,
      env: String => Option[String],
  ): Either[String, String] =
    target.tokenEnv match
      case None => Right(baseToken)
      case Some(varName) =>
        env(varName).map(_.trim).filter(_.nonEmpty) match
          case Some(value) => Right(value)
          case None =>
            Left(
              s"sdp: target '$name' authenticates with the environment variable '$varName', " +
                s"but '$varName' is unset or empty. Export it in the shell that runs sbt " +
                "(the value is read at task time; it is never stored in build.sbt and never logged)."
            )

  /** Transport settings → the connect [[TransportConfig]]. Plaintext + anonymous
    * unless asked otherwise; an empty token means anonymous, and the token is
    * never logged (`TransportConfig` redacts it in `toString`). */
  private[plugin] def transportConfig(
      useTls: Boolean,
      token: String,
      deadlineSeconds: Int,
  ): TransportConfig =
    TransportConfig(
      useTls = useTls,
      token = Some(token).map(_.trim).filter(_.nonEmpty),
      deadlineSeconds = deadlineSeconds.toLong,
    )
