package dev.sdp.plugin.fixture

import dev.sdp.core.GraphFragment
import dev.sdp.dsl.*

/** Compiled DSL declarations whose `.tasty` the scanner spec reads back.
  *
  * This object is the spike's subject: when this file compiles, the macros
  * embed `SdpMeta.embed("...")` literals here, and the spec verifies
  * tasty-query can recover them from this module's test-classes output.
  */
object SpikeFixture:

  val bronze: GraphFragment = table("spike_bronze")

  val silver: GraphFragment = streamingTable("spike_silver") { ctx =>
    ctx.readStream.table("spike_bronze")
  }

  /** Declaration-based discovery semantics: this fragment lives inside a
    * `def` that no test ever calls — the scanner must find it anyway.
    */
  def neverCalled(): GraphFragment =
    table("spike_unreferenced")
