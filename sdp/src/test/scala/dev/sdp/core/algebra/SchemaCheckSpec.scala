package dev.sdp.core.algebra

import zio.test.*

import SchemaCheck.{Shape, SchemaError}

object SchemaCheckSpec extends ZIOSpecDefault:

  private val rateSource =
    Rel.DataSource("rate", Map.empty, streaming = true,
      schema = List("timestamp" -> ColType.Timestamp, "value" -> ColType.I64))

  private def noUpstream: String => Shape = _ => Shape.Unknown

  def spec = suite("SchemaCheck")(
    test("declared source schemas are Known; range has its built-in shape") {
      assertTrue(
        SchemaCheck.infer(rateSource, noUpstream)._2 ==
          Shape.Known(List("timestamp" -> ColType.Timestamp, "value" -> ColType.I64)),
        SchemaCheck.infer(Rel.Range(0, 10, 1), noUpstream)._2 ==
          Shape.Known(List("id" -> ColType.I64)),
      )
    },
    test("a typo'd column is an error listing the available columns") {
      val plan = Rel.Filter(rateSource, Ex.Fn(">", List(Ex.Col("vlaue"), Ex.Lit(LitValue.I64(0)))))
      val (errors, _) = SchemaCheck.infer(plan, noUpstream)
      assertTrue(errors == List(SchemaError.UnknownColumn("vlaue", List("timestamp", "value"))))
    },
    test("shapes transform through project, withColumn, rename, drop, toDF") {
      val plan = Rel.ToDF(
        Rel.Drop(
          Rel.WithColumnsRenamed(
            Rel.WithColumns(
              Rel.Project(rateSource, List(Ex.Col("value"), Ex.Alias(Ex.Col("timestamp"), "ts"))),
              List("doubled" -> Ex.Fn("*", List(Ex.Col("value"), Ex.Lit(LitValue.I32(2))))),
            ),
            List("ts" -> "event_time"),
          ),
          List("event_time"),
        ),
        List("v", "d"),
      )
      val (errors, shape) = SchemaCheck.infer(plan, noUpstream)
      assertTrue(
        errors.isEmpty,
        shape.columnNames == Some(List("v", "d")),
      )
    },
    test("gradual: anything downstream of a SQL body is unchecked") {
      val plan = Rel.Filter(
        Rel.Sql("SELECT whatever FROM wherever"),
        Ex.Fn(">", List(Ex.Col("made_up_column"), Ex.Lit(LitValue.I64(0)))),
      )
      val (errors, shape) = SchemaCheck.infer(plan, noUpstream)
      assertTrue(errors.isEmpty, shape == Shape.Unknown)
    },
    test("semi/anti joins keep only the left shape; inner merges both") {
      val left  = Rel.Project(Rel.Range(0, 1, 1), List(Ex.Alias(Ex.Col("id"), "l")))
      val right = Rel.Project(Rel.Range(0, 1, 1), List(Ex.Alias(Ex.Col("id"), "r")))
      def join(jt: JoinType) = SchemaCheck.infer(Rel.Join(left, right, None, jt), noUpstream)._2
      assertTrue(
        join(JoinType.LeftSemi).columnNames == Some(List("l")),
        join(JoinType.Inner).columnNames == Some(List("l", "r")),
      )
    },
    test("qualified column references (a.b) are left to the server") {
      val plan = Rel.Filter(rateSource, Ex.Col("alias.anything"))
      assertTrue(SchemaCheck.infer(plan, noUpstream)._1.isEmpty)
    },
  )
