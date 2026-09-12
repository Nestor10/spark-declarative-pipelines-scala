package dev.sdp.core

import dev.sdp.core.algebra.{ColType, Ex, LitValue, Rel, SubqueryKind}
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
    suite("completeness (P3.1)")(
      test("an inline table inside a SUBQUERY is found — the cap was bypassable") {
        // `filter(exists(<huge inline table>))`: a relation in EXPRESSION
        // position. The structural walk never enters one, so this rode the
        // fragment string and the manifest uncapped.
        val oversized = strTable(InlineDataGuard.MaxRows + 1)
        val hidden = Rel.Filter(
          Rel.Range(0, 10, 1),
          Ex.Subquery(oversized, SubqueryKind.Exists),
        )
        assertTrue(InlineDataGuard.check("f", hidden).size == 1)
      },
      test("…at any expression depth, and through scalar/in as well") {
        val oversized = strTable(InlineDataGuard.MaxRows + 1)
        // buried under a function argument inside a lambda body
        val deep = Rel.Project(
          Rel.Range(0, 1, 1),
          List(
            Ex.Fn(
              "coalesce",
              List(Ex.Lam(List("x"), Ex.Subquery(oversized, SubqueryKind.Scalar))),
              distinct = false,
            )
          ),
        )
        val inList = Rel.Filter(
          Rel.Range(0, 1, 1),
          Ex.Subquery(oversized, SubqueryKind.In(List(Ex.Col("id")))),
        )
        assertTrue(
          InlineDataGuard.check("f", deep).size == 1,
          InlineDataGuard.check("f", inList).size == 1,
        )
      },
      test("a subquery's inline table is counted ONCE, not once per traversal") {
        val small = Rel.Filter(Rel.Range(0, 1, 1), Ex.Subquery(strTable(3), SubqueryKind.Exists))
        assertTrue(
          InlineDataGuard.check("f", small).isEmpty,
          Flow.allRelsDeep(small).count { case _: Rel.LocalData => true; case _ => false } == 1,
        )
      },
      test("multi-byte cells are measured in UTF-8 bytes, not UTF-16 units") {
        // 3 bytes each in UTF-8, one `char` each in UTF-16: measured by
        // `length` this table read as a third of its true size and passed.
        val cell  = "日" * 30000 // 30_000 chars, 90_000 bytes — over the 64 KiB cap
        val table: Rel.LocalData =
          Rel.LocalData(List("a" -> ColType.Str), List(List(LitValue.Str(cell))))
        assertTrue(
          cell.length.toLong <= InlineDataGuard.MaxBytes, // the old estimate: "fine"
          InlineDataGuard.estimatedBytes(table) == 90000L,
          InlineDataGuard.check("f", table).size == 1,
        )
      },
      test("an ASCII table straddling the cap is unaffected by the change") {
        val under: Rel.LocalData = Rel.LocalData(
          List("a" -> ColType.Str),
          List(List(LitValue.Str("x" * InlineDataGuard.MaxBytes.toInt))),
        )
        assertTrue(
          InlineDataGuard.estimatedBytes(under) == InlineDataGuard.MaxBytes,
          InlineDataGuard.check("f", under).isEmpty,
        )
      },
    ),
  )
