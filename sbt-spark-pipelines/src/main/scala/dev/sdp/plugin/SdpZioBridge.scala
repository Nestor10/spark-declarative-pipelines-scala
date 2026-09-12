package dev.sdp.plugin

import java.util.concurrent.{Executors, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import dev.sdp.app.{GraphValidation, ManifestAssembly}
import dev.sdp.core.{GraphFragment, PipelineManifest, PipelineValidationError}
import zio.*

/** The ZIO ⇄ sbt boundary, kept deliberately tiny and brutally disciplined.
  *
  * sbt tasks run on sbt's own thread pools inside a long-lived JVM. Two rules
  * from `context/onion_architecture_sbt_zio_plugin.md` apply:
  *
  *   1. Never use `Runtime.default` here — its global scheduler outlives the
  *      task and pins task classloaders, leaking Metaspace across repeated
  *      invocations in a warm sbt server.
  *   2. Build a task-scoped runtime on an isolated executor and tear both
  *      down in `finally`, so every invocation leaves the JVM exactly as it
  *      found it.
  *
  * And one learned here (P3.2): *isolated* must not mean *shared between the
  * async and blocking executors*. See [[run]].
  */
private[plugin] object SdpZioBridge:

  /** Run the assembly pipeline synchronously on an isolated runtime.
    * Validation failures come back as `Left` (expected, typed); anything
    * else escapes as a defect.
    */
  def assemble(
      fragments: List[GraphFragment]
  ): Either[::[PipelineValidationError], PipelineManifest] =
    run(
      ManifestAssembly
        .assemble(fragments)
        .provide(ManifestAssembly.live, GraphValidation.live)
    )

  /** Run any typed effect synchronously on a task-scoped isolated runtime.
    * The expected failures of `effect` come back as `Left`; defects still
    * escape as exceptions (sbt renders them as task crashes, which is what
    * a defect deserves).
    */
  def run[E, A](effect: IO[E, A]): Either[E, A] =
    // TWO executors, not one (P3.2). A single fixed pool served as both the
    // async and the blocking executor, which is a latent deadlock: every
    // Spark Connect pull is `attemptBlockingInterrupt`, so two of them park
    // both threads and the fiber that would complete them — the drain-vs-cancel
    // race's timer continuation, the `.tap` that logs an event — has nowhere
    // left to run. Splitting is the whole fix: the async executor stays small
    // and is never allowed to block, while blocking work gets a pool that
    // GROWS (cached) so parking N threads can never starve the runtime.
    val async    = Executors.newFixedThreadPool(AsyncThreads, daemonThreads("sdp-zio-async"))
    val blocking = Executors.newCachedThreadPool(daemonThreads("sdp-zio-blocking"))
    try
      val isolated = Unsafe.unsafe { implicit u =>
        Runtime.unsafe.fromLayer(
          Runtime.setExecutor(Executor.fromJavaExecutor(async)) ++
            Runtime.setBlockingExecutor(Executor.fromJavaExecutor(blocking))
        )
      }
      try
        Unsafe.unsafe { implicit u =>
          isolated.unsafe.run(effect.either).getOrThrowFiberFailure()
        }
      finally Unsafe.unsafe { implicit u => isolated.unsafe.shutdown() }
    finally
      // Forceful + bounded teardown, BOTH pools: `isolated.shutdown()`
      // interrupts the fibers and runs the scoped finalizers (gRPC
      // `channel.shutdownNow()`), so the threads are idle by now.
      // `shutdownNow()` first on both, THEN the awaits, so the two bounded
      // waits overlap instead of summing. A warm sbt server never accrues our
      // threads, even if a blocking call straggled.
      val _ = async.shutdownNow()
      val _ = blocking.shutdownNow()
      val _ = async.awaitTermination(TeardownSeconds, TimeUnit.SECONDS)
      val _ = blocking.awaitTermination(TeardownSeconds, TimeUnit.SECONDS)

  /** The async executor only ever runs fiber steps, never a blocking call, so
    * it stays deliberately tiny — the point of the isolated runtime is a
    * minimal, disposable footprint inside a long-lived sbt server. Size is not
    * what fixes the deadlock; the split is. */
  private final val AsyncThreads = 2

  /** Bounded wait per pool — see the teardown comment in [[run]]. */
  private final val TeardownSeconds = 10L

  /** Named daemon threads: named so a thread dump in a warm sbt server says
    * whose they are, daemon so a straggler can never keep the JVM alive. */
  private def daemonThreads(prefix: String): ThreadFactory =
    val counter = new AtomicInteger(0)
    (r: Runnable) =>
      val t = new Thread(r, s"$prefix-${counter.incrementAndGet()}")
      t.setDaemon(true)
      t
