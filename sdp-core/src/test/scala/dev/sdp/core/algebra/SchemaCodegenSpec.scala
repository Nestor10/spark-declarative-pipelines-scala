package dev.sdp.core.algebra

import zio.test.*

import SchemaCodegen.Entry

object SchemaCodegenSpec extends ZIOSpecDefault:

  def spec = suite("SchemaCodegen")(
    test("renders sorted, deterministic named-tuple aliases") {
      val entries = List(
        Entry("silver_rates", List("value" -> ColType.I64, "bucket" -> ColType.Unknown), "pipeline-inferred"),
        Entry("raw_events", List("timestamp" -> ColType.Timestamp, "value" -> ColType.I64), "pipeline-inferred"),
      )
      val out = SchemaCodegen.render("sdp.schemas", entries)
      assertTrue(
        out.contains("package sdp.schemas"),
        out.contains("type RawEvents = (timestamp: java.sql.Timestamp, value: Long)"),
        out.contains("type SilverRates = (value: Long, bucket: Any)"),
        out.indexOf("RawEvents") < out.indexOf("SilverRates"), // sorted by dataset
        out == SchemaCodegen.render("sdp.schemas", entries.reverse), // input order quotiented
      )
    },
    test("the generated source compiles and works with cols[S]") {
      // Pinned by hand: this is exactly the alias the renderer emits.
      type RawEvents = (timestamp: java.sql.Timestamp, value: Long)
      // compile-time proof that the alias shape is cols-compatible:
      val ok = scala.compiletime.testing.typeChecks(
        """
        import dev.sdp.dsl.*
        type RawEvents = (timestamp: java.sql.Timestamp, value: Long)
        val c = cols[RawEvents]
        """
      )
      assertTrue(ok)
    } @@ TestAspect.ignore, // dsl module not on core's test classpath; covered by TypedColsSpec + scripted
    test("hostile names sanitize: type name PascalCases, column names backtick") {
      val out = SchemaCodegen.render(
        "p",
        List(Entry("weird-dataset.name", List("normal" -> ColType.Str, "with space" -> ColType.I32), "catalog")),
      )
      assertTrue(
        out.contains("type WeirdDatasetName = "),
        out.contains("`with space`: Int"),
      )
    },
  )
