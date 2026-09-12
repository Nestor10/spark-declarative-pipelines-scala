package dev.sdp.plugin

import java.util.concurrent.{ArrayBlockingQueue, LinkedBlockingQueue, TimeUnit}

import scala.jdk.CollectionConverters.*

import zio.*
import zio.test.*

/** The ZIO ⇄ sbt boundary's two standing promises: it does not deadlock, and it
  * leaves no threads behind.
  *
  * These are hard to test from the outside because `SdpZioBridge.run` is
  * deliberately SYNCHRONOUS — it blocks the calling (sbt) thread until the
  * effect finishes. So each test runs it on its own daemon thread and waits on
  * a queue with a timeout: a regression shows up as "nothing arrived within N
  * seconds", which is exactly what a deadlock looks like to a user staring at
  * a wedged `sdpRun`.
  */
object SdpZioBridgeSpec extends ZIOSpecDefault:

  /** Run the bridge off-thread and wait, bounded. `None` = it never returned. */
  private def runOffThread[E, A](effect: IO[E, A], within: Duration): Task[Option[Either[E, A]]] =
    ZIO.attemptBlocking {
      val mailbox = new ArrayBlockingQueue[Either[E, A]](1)
      val thread  = new Thread(() => { val _ = mailbox.offer(SdpZioBridge.run(effect)) }, "bridge-under-test")
      thread.setDaemon(true) // a wedged bridge must not keep the test JVM alive
      thread.start()
      Option(mailbox.poll(within.toSeconds, TimeUnit.SECONDS))
    }

  /** How many of the bridge's own threads are alive right now. Compared
    * before/after rather than asserted to be zero: this JVM is a warm sbt
    * server shared with everything else the suite has run, and the claim under
    * test is "this invocation left nothing behind", not "nobody ever leaked". */
  private def bridgeThreadCount: Task[Int] =
    ZIO.attempt(Thread.getAllStackTraces.keySet.asScala.count(_.getName.startsWith("sdp-zio-")))

  def spec = suite("SdpZioBridge")(
    test("parked blocking effects cannot starve the async executor") {
      // THE regression, in one shape: more simultaneously-parked blocking
      // effects than the async executor has threads, plus a continuation that
      // must run to release them. On the old single-pool bridge (a fixed pool
      // of 2 doing double duty) the three `take()`s own every thread there is,
      // the fiber holding the `put`s never gets scheduled, and `run` never
      // returns. With the executors split, the blocking pool grows to three
      // and the async side is untouched.
      val parked  = new LinkedBlockingQueue[String]
      val release = new LinkedBlockingQueue[String]

      val blockingPull =
        ZIO.attemptBlocking {
          val _ = parked.offer("parked")
          release.take()
        }

      val effect: IO[Throwable, Int] =
        for
          fibers <- ZIO.foreach(1 to 3)(_ => blockingPull.fork)
          // Wait for all three to be parked. This is a FOURTH blocking effect:
          // on a pool of two it can never be scheduled, which is the deadlock.
          _ <- ZIO.attemptBlocking(parked.take()).repeatN(2)
          _ <- ZIO.attemptBlocking { (1 to 3).foreach(_ => release.put("go")) }
          _ <- ZIO.foreachDiscard(fibers)(_.join)
        yield fibers.size

      runOffThread(effect, 30.seconds).map { outcome =>
        assertTrue(outcome == Some(Right(3)))
      }
    },
    test("a blocking effect and an async continuation make progress together") {
      // The narrower shape from the drain-vs-cancel race: something parked on a
      // blocking pull, while a TIMER continuation on the async executor has to
      // fire to cancel it. `raceFirst` is the exact combinator `pushOrRun` uses.
      // Measured honestly: this one passes on the OLD single-pool shape too
      // (one parked pull still left a thread free). It is here as a guard on
      // the combination, not as the regression above.
      val never = new LinkedBlockingQueue[String]
      val effect: IO[Throwable, String] =
        ZIO
          // attemptBlockingINTERRUPT, as every Connect pull is: the loser of
          // the race has to be interruptible or the race cannot finish.
          .attemptBlockingInterrupt(never.take())
          .raceFirst(ZIO.sleep(200.millis).as("detached"))

      runOffThread(effect, 30.seconds).map { outcome =>
        assertTrue(outcome == Some(Right("detached")))
      }
    },
    test("both pools are gone when run returns — the Metaspace convention") {
      for
        before <- bridgeThreadCount
        result <- ZIO.attemptBlocking(SdpZioBridge.run(ZIO.succeed(1)))
        after  <- bridgeThreadCount
      yield assertTrue(result == Right(1), after == before)
    },
    test("an expected failure comes back as Left, and still tears both pools down") {
      for
        before <- bridgeThreadCount
        result <- ZIO.attemptBlocking(SdpZioBridge.run(ZIO.fail("nope")))
        after  <- bridgeThreadCount
      yield assertTrue(result == Left("nope"), after == before)
    },
  ) @@ TestAspect.withLiveClock @@ TestAspect.sequential
