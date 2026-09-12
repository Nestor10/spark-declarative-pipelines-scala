package dev.sdp.connect.conformance

/** Print the SDP **behavioral** conformance matrix — what the server does that
  * we depend on, and which of those behaviors a spec actually pins down.
  *
  * Companion to [[PrintCoverage]], which prints the *wire-surface* matrix. The
  * two answer different questions, and D9 is the record of what happens when
  * you mistake one for the other: the wire matrix said 100% while an entire
  * feature (external-table reads) was missing, because that feature is a
  * *usage distinction* in an existing message, not a message of its own.
  *
  * {{{
  * sbt 'sdp/Test/runMain dev.sdp.connect.conformance.PrintBehaviorCoverage'
  * }}}
  *
  * Generated, never hand-maintained as prose: the rows live in
  * [[BehaviorInventory]] (each with a Spark source anchor), and
  * `BehaviorCoverageSpec` fails if a row loses its anchor or names a spec class
  * that does not exist.
  */
object PrintBehaviorCoverage:
  def main(args: Array[String]): Unit =
    println()
    println(BehaviorInventory.report)
    println()
