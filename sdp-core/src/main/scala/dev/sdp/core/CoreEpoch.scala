package dev.sdp.core

/** Cache-correctness epoch — **bump on every sdp-core release.**
  *
  * Why this exists (verified by minimal repro, 2026-06-06): macros in
  * `sdp-runtime-dsl` *call* sdp-core code at expansion time, so a behavioral
  * change here can leave the dsl module's bytecode bit-identical — and
  * sbt 2's content-addressed action cache will then replay downstream
  * compiles with **stale macro-embedded constants**, surviving `clean`.
  *
  * `sdp-runtime-dsl` references this `final val` (a compile-time constant,
  * inlined into its classfiles/TASTy), so bumping it changes the dsl
  * module's content hash → every downstream cache key misses → macros
  * re-expand. The epoch is the release discipline that keeps warm caches
  * honest.
  */
object CoreEpoch:
  final val value = 4 // bumped 2026-06-08: DataSource.schemaDdl (source schema on the wire)
