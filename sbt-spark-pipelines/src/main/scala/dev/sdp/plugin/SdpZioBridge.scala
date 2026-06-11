package dev.sdp.plugin

import java.util.concurrent.{Executors, TimeUnit}

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
    val pool = Executors.newFixedThreadPool(2)
    try
      val isolated = Unsafe.unsafe { implicit u =>
        Runtime.unsafe.fromLayer(
          Runtime.setExecutor(Executor.fromJavaExecutor(pool)) ++
            Runtime.setBlockingExecutor(Executor.fromJavaExecutor(pool))
        )
      }
      try
        Unsafe.unsafe { implicit u =>
          isolated.unsafe.run(effect.either).getOrThrowFiberFailure()
        }
      finally Unsafe.unsafe { implicit u => isolated.unsafe.shutdown() }
    finally
      // Forceful + bounded teardown: `isolated.shutdown()` interrupts the
      // fibers and runs the scoped finalizers (gRPC `channel.shutdownNow()`),
      // so the pool threads are idle by now. `shutdownNow()` + a bounded await
      // guarantees they're actually gone before the task returns — a warm sbt
      // server never accrues our threads, even if a blocking call straggled.
      pool.shutdownNow()
      val _ = pool.awaitTermination(10, TimeUnit.SECONDS)
