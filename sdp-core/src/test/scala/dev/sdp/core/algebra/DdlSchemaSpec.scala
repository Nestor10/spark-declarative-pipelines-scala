package dev.sdp.core.algebra

import zio.test.*

/** Spark-fidelity surface: the DDL schema-string parser. Pure function — the
  * macro positions its `Left`s; everything interesting is testable here.
  */
object DdlSchemaSpec extends ZIOSpecDefault:

  def spec = suite("DdlSchema")(
    test("the checked types parse, case-insensitively, with Spark's aliases") {
      assertTrue(
        DdlSchema.parse("a BOOLEAN, b INT, c INTEGER, d BIGINT, e LONG, f DOUBLE, g STRING, h TIMESTAMP, i DATE") ==
          Right(List(
            "a" -> ColType.Bool,
            "b" -> ColType.I32,
            "c" -> ColType.I32,
            "d" -> ColType.I64,
            "e" -> ColType.I64,
            "f" -> ColType.F64,
            "g" -> ColType.Str,
            "h" -> ColType.Timestamp,
            "i" -> ColType.Date,
          )),
        DdlSchema.parse("value bigint, seen_at timestamp") ==
          Right(List("value" -> ColType.I64, "seen_at" -> ColType.Timestamp)),
      )
    },
    test("unchecked Spark types keep the name, type gradual (Unknown)") {
      assertTrue(
        DdlSchema.parse("amount DECIMAL(10,2), tags ARRAY<STRING>, kv MAP<STRING, INT>, s STRUCT<a: INT, b: STRING>") ==
          Right(List(
            "amount" -> ColType.Unknown,
            "tags"   -> ColType.Unknown,
            "kv"     -> ColType.Unknown,
            "s"      -> ColType.Unknown,
          ))
      )
    },
    test("backtick-quoted names, escaped backticks, NOT NULL suffix") {
      assertTrue(
        DdlSchema.parse("`weird name` STRING, `tick``ed` INT, id BIGINT NOT NULL") ==
          Right(List(
            "weird name" -> ColType.Str,
            "tick`ed"    -> ColType.I32,
            "id"         -> ColType.I64,
          ))
      )
    },
    test("malformed DDL is a readable Left") {
      assertTrue(
        DdlSchema.parse("").isLeft,
        DdlSchema.parse("a INT,, b STRING").isLeft,
        DdlSchema.parse("lonely").isLeft,
        DdlSchema.parse("a DECIMAL(10,2").isLeft,
        DdlSchema.parse("`unterminated STRING").isLeft,
      )
    },
  )
