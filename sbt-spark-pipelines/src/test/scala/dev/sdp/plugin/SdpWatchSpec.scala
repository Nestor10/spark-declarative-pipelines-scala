package dev.sdp.plugin

import zio.*
import zio.test.*

/** `sdpWatch`'s one load-bearing property: **a cycle runs the code that is on
  * disk NOW**, not the code that was there when the watch started.
  *
  * Until P3.2 the task read `sdpManifest.value` — one evaluation, at task
  * start — so a watch left running while its author edited the pipeline kept
  * re-triggering the old graph, silently. The fix is that `evaluate` is an
  * EFFECT inside the repeat rather than a value outside it, and that is exactly
  * what this spec pins: the loop's shape, with the classload-eval and the
  * server both replaced by counters, so the property is provable in
  * milliseconds and with no container.
  *
  * (The other half of the claim — that re-running the classload-eval over
  * recompiled classes really does yield a different graph — is the scripted
  * `caching` and `valid-pipeline` suites' job; they add and delete datasets
  * between `sdpManifest` runs and assert the manifest follows.)
  */
object SdpWatchSpec extends ZIOSpecDefault:

  /** Stop the endless loop deterministically: the cycle body fails once it has
    * recorded `n` cycles, which `.repeat` turns into the loop's own end. */
  private val Stop = "stop"

  def spec = suite("sdpWatch's loop")(
    test("the pipeline is re-evaluated on EVERY cycle, not once per watch") {
      for
        evaluations <- Ref.make(0)
        seen        <- Ref.make(Vector.empty[Int])
        // A pipeline that GROWS between cycles — the author adding a dataset
        // mid-watch. The value a cycle sees is the value at ITS evaluation.
        evaluate = evaluations.updateAndGet(_ + 1).map(n => 1 + n)
        cycle = (datasets: Int) =>
          seen.updateAndGet(_ :+ datasets).flatMap { all =>
            if all.size >= 3 then ZIO.fail(Stop) else ZIO.unit
          }
        exit  <- SparkPipelinesPlugin.watchEffect(evaluate, cycle, intervalSeconds = 0).exit
        count <- evaluations.get
        all   <- seen.get
      yield assertTrue(
        exit == Exit.fail(Stop),
        // three cycles, three evaluations — the old shape would have been ONE
        count == 3,
        // and each cycle saw the LATEST pipeline, not the first one
        all.toList == List(2, 3, 4),
      )
    },
    test("a failed evaluation ends the watch with its own message, before any run") {
      for
        ran  <- Ref.make(0)
        exit <- SparkPipelinesPlugin
          .watchEffect[Int](
            ZIO.fail("sdp: pipeline invalid — cycle detected: a → b → a"),
            _ => ran.update(_ + 1),
            intervalSeconds = 0,
          )
          .exit
        runs <- ran.get
      yield assertTrue(
        runs == 0,
        exit.causeOption.flatMap(_.failureOption).exists(_.contains("cycle detected")),
      )
    },
    test("a cycle that detaches instead of completing is still a cycle — the watch goes on") {
      // `registerAndDrain` reports a timed-out run as (graphId, false), which
      // the watch logs and treats as a completed CYCLE: one wedged streaming
      // source must not wedge the watch forever.
      for
        cycles <- Ref.make(0)
        detached = (_: Int) =>
          cycles.updateAndGet(_ + 1).flatMap(n => if n >= 4 then ZIO.fail(Stop) else ZIO.unit)
        exit <- SparkPipelinesPlugin.watchEffect(ZIO.succeed(1), detached, intervalSeconds = 0).exit
        n    <- cycles.get
      yield assertTrue(exit == Exit.fail(Stop), n == 4)
    },
  ) @@ TestAspect.withLiveClock
