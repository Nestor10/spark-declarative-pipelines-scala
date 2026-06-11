package dev.sdp.core

import dev.sdp.core.algebra.{ColType, Ex, LitValue, Rel}
import zio.test.*

object InlineDataGuardSpec extends ZIOSpecDefault:

  private def strTable(rows: Int, cell: String = "x"): Rel.LocalData =
    Rel.LocalData(List("a" -> ColType.Str), List.fill(rows)(List(LitValue.Str(cell))))

  def spec = suite("InlineDataGuard")(
    test("a small inline table passes") {
      assertTrue(InlineDataGuard.check("f", strTable(10)).isEmpty)
    },
    test("exceeding MaxRows fires exactly one InlineTableTooLarge") {
      val errs = InlineDataGuard.check("f", strTable(InlineDataGuard.MaxRows + 1))
      assertTrue(
        errs.size == 1,
        errs.head match
          case PipelineValidationError.InlineTableTooLarge("f", rows, _) => rows == InlineDataGuard.MaxRows + 1
          case _                                                         => false,
      )
    },
    test("exceeding MaxBytes fires even with few rows") {
      val big = Rel.LocalData(
        List("a" -> ColType.Str),
        List(List(LitValue.Str("x" * (InlineDataGuard.MaxBytes.toInt + 1)))),
      )
      assertTrue(InlineDataGuard.check("f", big).nonEmpty)
    },
    test("an inline table nested under transforms is still found") {
      val nested = Rel.Filter(
        Rel.Project(strTable(InlineDataGuard.MaxRows + 1), Nil),
        Ex.Lit(LitValue.Bool(true)),
      )
      assertTrue(InlineDataGuard.check("f", nested).size == 1)
    },
  )
