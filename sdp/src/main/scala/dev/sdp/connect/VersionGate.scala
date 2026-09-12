package dev.sdp.connect

import dev.sdp.core.{FlowDetails, PipelineManifest, ScdType}

import PipelinesRegistration.RegistrationError

/** The server-version handshake, as a **pure function**.
  *
  * Why this exists: proto3 silently DROPS unknown fields. A client pinned to
  * the 4.2 wire that sends a 4.2-only construct (AUTO CDC) to a 4.1 server
  * therefore does not get "unsupported" back — the server receives a
  * `DefineFlow` with *no* details and reports something confusing, or worse
  * accepts a half-message. The only defence is to ask the server what it is
  * before registering anything, and refuse with a sentence the author can act
  * on.
  *
  * Shape: resolving the version is I/O ([[PlanAnalysis.sparkVersionOn]]); the
  * DECISION is this object — `(serverVersion, manifest) => Either[error, Unit]`,
  * total, allocation-free and exhaustively testable off-container. The gate is
  * wired into [[PipelinesRegistration.register]] *before* `CreateDataflowGraph`,
  * so both `dry` and real runs (plugin `sdpDryRun`/`sdpRun`, `SdpApp run`) pass
  * through it. `validate`/`manifest` stay offline and never reach it.
  *
  * Policy (deliberate, in both directions):
  *   - **Lenient parse.** Only `major.minor` is consulted, read from the front
  *     of the string, so `4.1.1`, `4.2.0`, `4.2.0-preview1` and vendor spellings
  *     like `4.1.0-amzn-0` all resolve.
  *   - **Unparseable ⇒ PROCEED.** Forks and vendors report odd strings; a
  *     client that refuses to run against an unrecognised version is worse than
  *     one that tries. The caller logs a warning instead (see
  *     [[ServerVersion.parse]] returning `None`).
  *   - **Parseable but too old ⇒ HARD FAIL**, typed, naming the construct, the
  *     requirement, the endpoint and what the server reported.
  */
object VersionGate:

  /** A Spark server version reduced to what gating actually depends on:
    * `major.minor`. Patch level and qualifiers never carry a wire change. */
  final case class ServerVersion(major: Int, minor: Int):
    override def toString: String = s"$major.$minor"

  object ServerVersion:

    /** Lenient parse: the leading `major.minor` of whatever the server said.
      *
      * `None` means "unrecognised", which the gate treats as *proceed* — never
      * as *too old*. Anything before the digits (a stray `v`) and everything
      * after the minor (patch, `-preview1`, `-amzn-0`, build metadata) is
      * ignored by construction.
      */
    def parse(raw: String): Option[ServerVersion] =
      val trimmed = raw.trim.stripPrefix("v")
      LeadingMajorMinor.findPrefixMatchOf(trimmed).flatMap { m =>
        for
          major <- m.group(1).toIntOption
          minor <- m.group(2).toIntOption
        yield ServerVersion(major, minor)
      }

    private val LeadingMajorMinor = """(\d+)\.(\d+)""".r

    /** Ordering is lexicographic on `(major, minor)` — the only comparison the
      * gate makes ("is the server at least X.Y?"). */
    given Ordering[ServerVersion] = Ordering.by(v => (v.major, v.minor))

  /** A pipeline construct whose *wire* representation only exists from some
    * Spark version onward.
    *
    * **This enum is the single requirements map** — construct → minimum server
    * version — so a new gated construct is one `case` here plus one line in
    * [[constructsOf]], and nothing else in the codebase has an opinion about
    * versions. (The `scdType` match in [[constructsOf]] is exhaustive, so a new
    * SCD type cannot be added to the core ADT without landing here, which is
    * the point.)
    *
    * Note the division of labour with [[Scd2Wire]]: this map is about the
    * *server*, that gate is about the *artifact we compile against*. Both must
    * pass, and an SCD2 pipeline currently fails the second one first (no
    * released proto carries SCD2 yet).
    */
  enum Construct(val label: String, val minimumVersion: ServerVersion):
    case AutoCdcScd1
        extends Construct("AUTO CDC flow (SCD type 1)", ServerVersion(4, 2))

    /** SCD type 2 is master-only upstream (SPARK-58247, no release tag contains
      * it as of 2026-09-12): the proto fields land in a Spark **4.3**, and the
      * engine-side `Scd2BatchProcessor` with them. 4.3 is therefore the
      * earliest server that can honor it; if upstream backports into a 4.2.x,
      * this number is the one line to change. */
    case AutoCdcScd2
        extends Construct("AUTO CDC flow (SCD type 2)", ServerVersion(4, 3))

  /** One *use* of a gated construct: which flow, writing which target. Named so
    * the error can point at the author's own flow rather than a category. */
  final case class Usage(construct: Construct, flowName: String, target: String):
    def describe: String = s"${construct.label} '$flowName' (target '$target')"

  /** Every gated construct the manifest actually uses, in manifest order
    * (already canonically sorted, so the message is deterministic).
    *
    * `WriteRelation` flows are the v2 shape — nothing in them is version-gated,
    * so they contribute nothing. */
  def constructsOf(manifest: PipelineManifest): List[Usage] =
    manifest.flows.flatMap { flow =>
      flow.details match
        case _: FlowDetails.WriteRelation => None
        case cdc: FlowDetails.AutoCdc =>
          val construct = cdc.scdType match
            case ScdType.Scd1 => Construct.AutoCdcScd1
            case ScdType.Scd2 => Construct.AutoCdcScd2
          Some(Usage(construct, flow.name, flow.target))
    }

  /** The gate. `Right(())` = safe to register.
    *
    * @param serverVersion what the server reported, verbatim (`AnalyzePlan`'s
    *                      `SparkVersion`) — unparseable strings pass
    * @param manifest      the assembled pipeline
    * @param endpoint      `sc://host:port`, named in the error so a multi-target
    *                      build says *which* server is too old
    */
  def check(
      serverVersion: String,
      manifest: PipelineManifest,
      endpoint: String,
  ): Either[RegistrationError, Unit] =
    ServerVersion.parse(serverVersion) match
      // Unrecognised version: never block. The caller has already warned.
      case None => Right(())
      case Some(server) =>
        val ord        = summon[Ordering[ServerVersion]]
        val tooNew     = constructsOf(manifest).filter(u => ord.lt(server, u.construct.minimumVersion))
        if tooNew.isEmpty then Right(())
        else Left(RegistrationError.ServerTooOld(message(tooNew, serverVersion, endpoint)))

  /** One line per offending flow — the author fixes all of them in one pass
    * instead of rediscovering the next one on the next run. */
  private def message(
      usages: List[Usage],
      reported: String,
      endpoint: String,
  ): String =
    val lines = usages.map { u =>
      s"${u.describe} needs a Spark ${u.construct.minimumVersion}+ server; " +
        s"$endpoint reports $reported"
    }
    if lines.sizeIs == 1 then lines.head else lines.mkString("\n  - ", "\n  - ", "")
