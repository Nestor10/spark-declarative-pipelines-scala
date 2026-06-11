package dev.sdp.core

import zio.test.*

/** The pure run-progress parser — the structured substrate a live DAG view
  * will consume. Anchored to the exact message wording the live 4.1.2 server
  * emits (observed 2026-06-08). */
object RunProgressSpec extends ZIOSpecDefault:

  def spec = suite("RunProgress.parse")(
    test("per-flow lifecycle states with the flow id extracted") {
      val cases = List(
        "Flow spark_catalog.default.nums_run is QUEUED."   -> FlowState.Queued,
        "Flow spark_catalog.default.nums_run is PLANNING."  -> FlowState.Planning,
        "Flow spark_catalog.default.nums_run is STARTING."  -> FlowState.Starting,
        "Flow spark_catalog.default.nums_run is RUNNING."   -> FlowState.Running,
        "Flow spark_catalog.default.nums_run has COMPLETED." -> FlowState.Completed,
      )
      assertTrue(cases.forall { case (msg, st) =>
        val p = RunProgress.parse(msg)
        p.state == st && p.flow.contains("spark_catalog.default.nums_run")
      })
    },
    test("run-level events carry no flow") {
      val p = RunProgress.parse("Run is COMPLETED.")
      assertTrue(p.state == FlowState.Completed, p.flow.isEmpty)
    },
    test("failure messages with a quoted flow id parse to Failed") {
      val p = RunProgress.parse(
        "Failed to resolve flow: 'spark_catalog.default.orders_enriched'."
      )
      assertTrue(p.state == FlowState.Failed, p.flow.contains("spark_catalog.default.orders_enriched"))
    },
    test("unrecognized messages are Unknown but never lossy") {
      val p = RunProgress.parse("some novel server message")
      assertTrue(p.state == FlowState.Unknown, p.raw == "some novel server message", p.flow.isEmpty)
    },
    test("terminal-state classification") {
      assertTrue(
        FlowState.Completed.isTerminal,
        FlowState.Failed.isTerminal,
        FlowState.Excluded.isTerminal,
        !FlowState.Running.isTerminal,
        !FlowState.Queued.isTerminal,
      )
    },
  )
