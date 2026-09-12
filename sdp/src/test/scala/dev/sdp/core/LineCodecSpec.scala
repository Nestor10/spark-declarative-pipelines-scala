package dev.sdp.core

import dev.sdp.core.algebra.{Rel, RelCodec}
import zio.test.*

/** Totality of the line/algebra codecs (review item 4).
  *
  * The fragment string is the cross-classloader, cross-version boundary: a
  * plugin and a library out of lockstep, or a hand-edited manifest, can hand
  * the parser anything. `RelCodec`'s contract is "total parsing with the
  * offending input in the error", but `URLDecoder.decode` throws on a malformed
  * escape (`%zz`), so every decode position used to be a latent exception —
  * and `parseLine` additionally discarded the inner diagnostic via `.toOption`.
  *
  * These tests pin both halves: malformed input in EVERY decode position
  * returns a `Left` (never throws), and the message names the offending input.
  */
object LineCodecSpec extends ZIOSpecDefault:

  /** Run `f`, reporting a throw as a failure rather than letting it escape. */
  private def total[A](f: => Either[String, A]): Either[String, A] =
    try f
    catch case e: Throwable => Left(s"THREW: $e")

  private def isError(result: Either[String, ?], mustMention: String): Boolean =
    result match
      case Left(message) => !message.startsWith("THREW:") && message.contains(mustMention)
      case Right(_)      => false

  def spec = suite("codec totality at the version-skew boundary")(
    test("decode is total: a malformed escape is a Left naming the input") {
      assertTrue(
        LineCodec.decode("plain") == Right("plain"),
        LineCodec.decode("a%20b") == Right("a b"),
        isError(total(LineCodec.decode("bad%zz")), "bad%zz"),
        isError(total(LineCodec.decode("truncated%a")), "truncated%a"),
      )
    },
    test("a malformed escape in a NODE line is an error, not a throw") {
      assertTrue(
        isError(total(LineCodec.parseLine("node|bad%zz|table|delta")), "bad%zz"),
        isError(total(LineCodec.parseLine("node|ok|table|fmt%zz")), "fmt%zz"),
        isError(total(LineCodec.parseLine("node|bad%zz|materialized-view|SELECT")), "bad%zz"),
        isError(total(LineCodec.parseLine("node|bad%zz|external|")), "bad%zz"),
      )
    },
    test("a malformed escape in an EDGE line is an error, not a throw") {
      assertTrue(
        isError(total(LineCodec.parseLine("edge|bad%zz|silver")), "bad%zz"),
        isError(total(LineCodec.parseLine("edge|bronze|bad%zz")), "bad%zz"),
      )
    },
    test("a malformed escape in a FLOW line is an error, not a throw (v2 and v3)") {
      val rel = LineCodec.enc(RelCodec.render(Rel.NamedTable("t", streaming = false)))
      assertTrue(
        isError(total(LineCodec.parseLine(s"flow|bad%zz|silver|$rel")), "bad%zz"),
        isError(total(LineCodec.parseLine(s"flow|f|bad%zz|$rel")), "bad%zz"),
        // v3: fifth field present, details field malformed
        isError(total(LineCodec.parseLine("flow|f|silver|bad%zz|f")), "bad%zz"),
        // v3: the once flag must be t/f, and the diagnostic says so
        isError(total(LineCodec.parseLine(s"flow|f|silver|$rel|maybe")), "once flag"),
      )
    },
    test("a malformed escape INSIDE the encoded Rel is an error, not a throw") {
      // the fourth field decodes fine, but an atom inside the s-expression does not
      val innerBad = LineCodec.enc("(read bad%zz batch)")
      assertTrue(
        isError(total(RelCodec.parse("(read bad%zz batch)")), "bad%zz"),
        isError(total(RelCodec.parseEx("(col bad%zz)")), "bad%zz"),
        isError(total(LineCodec.parseLine(s"flow|f|silver|$innerBad")), "bad%zz"),
        // …and the flow line keeps the INNER diagnostic instead of swallowing it
        total(LineCodec.parseLine(s"flow|f|silver|$innerBad")) match
          case Left(message) => message.contains("flow 'f'")
          case Right(_)      => false,
      )
    },
    test("a malformed escape inside AUTO CDC details is an error, not a throw") {
      val badDetails = LineCodec.enc("autocdc bad%zz scd1 keys 0 seq %28col+k%29 del 0 trunc 0 " +
        "cols 0 except 0 ignidx 0 ignexc 0")
      assertTrue(isError(total(LineCodec.parseLine(s"flow|f|silver|$badDetails|f")), "bad%zz"))
    },
    test("GraphFragment.parse propagates the diagnostic instead of just the line") {
      val result = total(GraphFragment.parse("node|bad%zz|table|delta"))
      assertTrue(isError(result, "malformed percent-encoding"), isError(result, "bad%zz"))
    },
    test("an unrecognized line shape says how many fields it had") {
      assertTrue(isError(total(LineCodec.parseLine("garbage line")), "1 fields"))
    },
    test("well-formed lines still parse (no regression in the happy path)") {
      assertTrue(
        LineCodec.parseLine("node|silver|streaming-table|delta") ==
          Right(LineCodec.ParsedLine.NodeLine(PipelineNode.StreamingTable("silver", "delta"))),
        LineCodec.parseLine("edge|bronze|silver") ==
          Right(LineCodec.ParsedLine.EdgeLine(DependencyEdge("bronze", "silver"))),
      )
    },
  )
