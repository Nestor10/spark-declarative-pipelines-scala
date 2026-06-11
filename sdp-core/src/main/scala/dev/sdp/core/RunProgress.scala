package dev.sdp.core

/** A single run-progress event, parsed from a Spark Connect `PipelineEvent`'s
  * human message.
  *
  * Why parsing, and why here: Spark 4.1.2's `PipelineEvent` carries only a
  * `timestamp` + a `message` string (e.g. "Flow spark_catalog.default.foo is
  * RUNNING.") — there is no structured per-flow status field yet. So progress
  * must be recovered from the message. Keeping that parse in ONE pure place
  * (the domain) means: it's unit-testable without a server, every consumer
  * (live log today, an ANSI DAG renderer tomorrow) sees structured data, and
  * when Spark adds structured event fields we swap the parser, not its callers.
  *
  * Pure Domain Core: no Spark types, no ZIO — just `String => RunProgress`.
  * The coupling to Spark's message wording is isolated to [[RunProgress.parse]].
  */
final case class RunProgress(flow: Option[String], state: FlowState, raw: String)

enum FlowState:
  case Queued, Planning, Starting, Running, Completed, Idle, Excluded, Failed, Unknown

  /** A terminal state for a single flow (no further transitions expected). */
  def isTerminal: Boolean = this match
    case Completed | Excluded | Failed => true
    case _                             => false

object RunProgress:

  /** Parse one event message into structured progress. Unrecognized messages
    * become `FlowState.Unknown` with the raw text preserved — never lossy. */
  def parse(message: String): RunProgress =
    val m     = message.trim
    val lower = m.toLowerCase
    val state =
      if lower.contains("queued") then FlowState.Queued
      else if lower.contains("planning") then FlowState.Planning
      else if lower.contains("starting") then FlowState.Starting
      else if lower.contains("running") then FlowState.Running
      else if lower.contains("completed") then FlowState.Completed
      else if lower.contains("idle") then FlowState.Idle
      else if lower.contains("excluded") then FlowState.Excluded
      else if lower.contains("failed") || lower.contains("error") then FlowState.Failed
      else FlowState.Unknown
    RunProgress(flowName(m), state, m)

  /** Pull the flow identifier out of "Flow <id> is/has ..." or "... flow
    * '<id>' ..." — `None` for run-level events ("Run is COMPLETED."). */
  private def flowName(message: String): Option[String] =
    val FlowIs   = """(?i).*\bflow\s+'?([\w.${}]+)'?\s+(?:is|has)\b.*""".r
    val FlowWord = """(?i).*\bflow[: ]+'([\w.${}]+)'.*""".r
    message match
      case FlowIs(id)   => Some(id)
      case FlowWord(id) => Some(id)
      case _            => None
