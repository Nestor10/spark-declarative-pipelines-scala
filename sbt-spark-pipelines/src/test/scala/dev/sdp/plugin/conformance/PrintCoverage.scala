package dev.sdp.plugin.conformance

/** Print the wire-surface coverage matrix: what we emit, what we don't, per
  * tier, plus untriaged counts. The numbers are *generated* from the pinned
  * Spark artifact's protobuf descriptors and the oracle-verified claim set —
  * never hand-maintained.
  *
  * {{{
  * sbt 'sbtSparkPipelines/Test/runMain dev.sdp.plugin.conformance.PrintCoverage'
  * }}}
  */
object PrintCoverage:
  def main(args: Array[String]): Unit =
    println()
    println(SupportedCapabilities.report)
    println()
