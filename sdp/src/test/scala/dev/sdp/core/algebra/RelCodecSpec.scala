package dev.sdp.core.algebra

import zio.test.*

object RelCodecSpec extends ZIOSpecDefault:

  /** One tree exercising every Rel case and every Ex/LitValue case.
    * `lazy` so the forward reference to `innerSink` resolves at use time.
    */
  private lazy val kitchenSink: Rel =
    Rel.WithColumnsRenamed(
      Rel.WithColumns(
        Rel.ToDF(
          Rel.SubqueryAlias(
            Rel.SetOp(
              Rel.Drop(
                Rel.Deduplicate(
                  Rel.Tail(Rel.Offset(Rel.Range(0L, 1000L, 2L), 5), 100),
                  List("a", "weird col"),
                ),
                List("dropped"),
              ),
              Rel.Range(0L, 10L, 1L),
              SetOpType.Except,
              all = true,
            ),
            "aliased rel",
          ),
          List("x", "y"),
        ),
        List("doubled" -> Ex.Fn("*", List(Ex.Col("x"), Ex.Lit(LitValue.I32(2))))),
      ),
      List("y" -> "renamed y"),
    ) match
      case wrapped => Rel.Join(wrapped, innerSink, None, JoinType.Cross)

  private val innerSink: Rel =
    Rel.Limit(
      Rel.Sort(
        Rel.Aggregate(
          Rel.Join(
            Rel.Filter(
              Rel.Project(
                Rel.NamedTable("bronze orders", streaming = true), // space → encoding exercised
                List(
                  Ex.Col("id"),
                  Ex.Alias(Ex.Fn("+", List(Ex.Col("id"), Ex.Lit(LitValue.I32(1)))), "next id"),
                  Ex.Lit(LitValue.Str("weird|%7C(value)")),
                  Ex.Lit(LitValue.Null),
                  Ex.Lit(LitValue.Bool(true)),
                  Ex.Lit(LitValue.F64(3.14159)),
                ),
              ),
              Ex.Fn(">", List(Ex.Col("id"), Ex.Lit(LitValue.I64(10L)))),
            ),
            Rel.Sql("SELECT * FROM range(5) -- ünïcødé (with parens)"),
            Some(Ex.Fn("==", List(Ex.Col("l"), Ex.Col("r")))),
            JoinType.LeftOuter,
          ),
          groupBy = List(Ex.Col("parity")),
          aggregates = List(Ex.Alias(Ex.Fn("count", List(Ex.Col("id")), distinct = true), "n")),
        ),
        List(
          SortKey(Ex.Col("n"), descending = true, nullsFirst = Some(false)),
          SortKey(Ex.Col("parity")),
        ),
      ),
      42,
    )

  def spec = suite("RelCodec")(
    test("round-trip law over the kitchen sink: parse(render(t)) == Right(t)") {
      assertTrue(RelCodec.parse(RelCodec.render(kitchenSink)) == Right(kitchenSink))
    },
    test("render is deterministic") {
      assertTrue(RelCodec.render(kitchenSink) == RelCodec.render(kitchenSink))
    },
    test("join without condition round-trips (optional-last encoding)") {
      val t = Rel.Join(Rel.NamedTable("a", false), Rel.NamedTable("b", false), None, JoinType.Cross)
      assertTrue(RelCodec.parse(RelCodec.render(t)) == Right(t))
    },
    test("T1: cast, sample and hint round-trip") {
      val t = Rel.Hint(
        Rel.Sample(
          Rel.Project(
            Rel.Range(0, 100, 1),
            List(Ex.Alias(Ex.Cast(Ex.Col("id"), ColType.Str), "id_str")),
          ),
          0.25,
          Some(42L),
        ),
        "broadcast",
        List(Ex.Col("id_str")),
      )
      val noSeed = Rel.Sample(Rel.Range(0, 1, 1), 0.5, None)
      assertTrue(
        RelCodec.parse(RelCodec.render(t)) == Right(t),
        RelCodec.parse(RelCodec.render(noSeed)) == Right(noSeed),
      )
    },
    test("T1 batch 2: repartitions, NA ops and stat relations round-trip") {
      val base = Rel.Range(0, 100, 1)
      val trees = List(
        Rel.Repartition(base, 4, shuffle = true),
        Rel.Repartition(base, 1, shuffle = false),
        Rel.RepartitionByExpression(base, List(Ex.Col("id")), Some(8)),
        Rel.RepartitionByExpression(base, List(Ex.Col("id")), None),
        Rel.DropNa(base, List("a", "b")),
        Rel.DropNa(base, Nil),
        Rel.FillNa(base, List("a"), LitValue.I64(0L)),
        Rel.Describe(base, List("id")),
        Rel.Summary(base, List("count", "mean")),
        Rel.Crosstab(base, "a", "b"),
        Rel.Cov(base, "a", "b"),
        Rel.Corr(base, "a", "b"),
        Rel.FreqItems(base, List("a")),
      )
      assertTrue(trees.forall(t => RelCodec.parse(RelCodec.render(t)) == Right(t)))
    },
    test("T1 batch 3: unpivot, transpose, replace, sampleBy, quantile, metrics round-trip") {
      val base = Rel.Range(0, 100, 1)
      val trees = List(
        Rel.Unpivot(base, List(Ex.Col("id")), List(Ex.Col("a"), Ex.Col("b")), "var", "val"),
        Rel.Unpivot(base, List(Ex.Col("id")), Nil, "k", "v"),
        Rel.Transpose(base, List(Ex.Col("id"))),
        Rel.Replace(base, List("a"), List((LitValue.Str("old"), LitValue.Str("new")))),
        Rel.SampleBy(base, Ex.Col("id"), List((LitValue.I64(0L), 0.1), (LitValue.I64(1L), 0.9)), Some(7L)),
        Rel.ApproxQuantile(base, List("id"), List(0.25, 0.5, 0.75), 0.01),
        Rel.CollectMetrics(base, "checkpoint", List(Ex.Alias(Ex.Fn("count", List(Ex.Col("id"))), "n"))),
      )
      assertTrue(trees.forall(t => RelCodec.parse(RelCodec.render(t)) == Right(t)))
    },
    test("T1 final: asof/lateral joins, parse, toSchema, localrel, show/html, tvf, catalog round-trip") {
      val base   = Rel.Range(0, 100, 1)
      val schema = List("ts" -> ColType.Timestamp, "v" -> ColType.I64)
      val trees = List(
        Rel.AsOfJoin(base, base, Ex.Col("id"), Ex.Col("id"), "inner", "backward", true, None),
        Rel.AsOfJoin(base, base, Ex.Col("id"), Ex.Col("id"), "left", "nearest", false,
          Some(Ex.Lit(LitValue.I64(5L)))),
        Rel.LateralJoin(base, base, None, JoinType.Cross),
        Rel.LateralJoin(base, base, Some(Ex.Fn("==", List(Ex.Col("a"), Ex.Col("b")))), JoinType.Inner),
        Rel.Parse(Rel.Sql("SELECT '{}' AS value"), ParseFormat.Json, Map("mode" -> "PERMISSIVE")),
        Rel.ToSchema(base, schema),
        Rel.LocalRelation(schema),
        Rel.LocalData(
          List("id" -> ColType.I32, "name" -> ColType.Str),
          List(List(LitValue.I32(1), LitValue.Str("North")), List(LitValue.I32(2), LitValue.Str("South"))),
        ),
        // escaping: every ColType token + a string with codec-significant chars
        Rel.LocalData(
          List(
            "b" -> ColType.Bool, "i" -> ColType.I32, "l" -> ColType.I64, "d" -> ColType.F64,
            "s" -> ColType.Str, "ts" -> ColType.Timestamp, "dt" -> ColType.Date,
          ),
          List(List(
            LitValue.Bool(true), LitValue.I32(7), LitValue.I64(9L), LitValue.F64(1.5),
            LitValue.Str("a (b) c\nd \\e ' \" %28"), LitValue.Null, LitValue.Null,
          )),
        ),
        Rel.ShowString(base, 10, 20, vertical = true),
        Rel.HtmlString(base, 5, 0),
        Rel.Tvf("range", List(Ex.Lit(LitValue.I64(5L)))),
        Rel.Catalog(CatalogOp.CurrentDatabase),
        Rel.Catalog(CatalogOp.ListDatabases),
        Rel.Catalog(CatalogOp.ListTables),
      )
      assertTrue(trees.forall(t => RelCodec.parse(RelCodec.render(t)) == Right(t)))
    },
    test("expression batch: expr/star/extract/regex/callfn/window round-trip") {
      val base = Rel.Range(0, 100, 1)
      val windowed = Ex.Window(
        Ex.Fn("row_number", Nil),
        List(Ex.Col("k")),
        List(SortKey(Ex.Col("ts"), descending = true)),
        Some(WindowFrame(rowFrame = true, FrameBoundary.Unbounded, FrameBoundary.CurrentRow)),
      )
      val trees = List(
        Rel.Project(base, List(Ex.ExprString("id % 7"))),
        Rel.Project(base, List(Ex.Star(None), Ex.Star(Some("t")))),
        Rel.Project(base, List(Ex.ExtractValue(Ex.Col("m"), Ex.Lit(LitValue.Str("k"))))),
        Rel.Project(base, List(Ex.ColRegex("`id.*`"))),
        Rel.Project(base, List(Ex.CallFn("upper", List(Ex.Col("s"))))),
        Rel.Project(base, List(Ex.Alias(windowed, "rn"))),
        Rel.Project(base, List(Ex.Window(Ex.Fn("sum", List(Ex.Col("v"))), Nil, Nil, None))),
        Rel.Project(base, List(Ex.Window(
          Ex.Fn("sum", List(Ex.Col("v"))),
          List(Ex.Col("k")),
          List(SortKey(Ex.Col("ts"))),
          Some(WindowFrame(rowFrame = false, FrameBoundary.Value(Ex.Lit(LitValue.I64(-3L))), FrameBoundary.CurrentRow)),
        ))),
      )
      assertTrue(trees.forall(t => RelCodec.parse(RelCodec.render(t)) == Right(t)))
    },
    test("lambdas and subqueries round-trip (relations inside expressions)") {
      val base = Rel.Range(0, 10, 1)
      val trees = List(
        Rel.Project(base, List(
          Ex.Fn("transform", List(
            Ex.Col("xs"),
            Ex.Lam(List("x"), Ex.Fn("+", List(Ex.LamVar("x"), Ex.Lit(LitValue.I32(1))))),
          )),
        )),
        Rel.Project(base, List(
          Ex.Fn("zip_with", List(
            Ex.Col("a"), Ex.Col("b"),
            Ex.Lam(List("x", "y"), Ex.Fn("+", List(Ex.LamVar("x"), Ex.LamVar("y")))),
          )),
        )),
        Rel.Filter(base, Ex.Subquery(Rel.Range(0, 1, 1), SubqueryKind.Exists)),
        Rel.Project(base, List(Ex.Alias(
          Ex.Subquery(Rel.Aggregate(base, Nil, List(Ex.Fn("max", List(Ex.Col("id"))))), SubqueryKind.Scalar),
          "m",
        ))),
        Rel.Filter(base, Ex.Subquery(
          Rel.Project(Rel.Range(0, 5, 1), List(Ex.Col("id"))),
          SubqueryKind.In(List(Ex.Col("id"))),
        )),
      )
      assertTrue(trees.forall(t => RelCodec.parse(RelCodec.render(t)) == Right(t)))
    },
    test("data source with options round-trips and renders deterministically") {
      val t = Rel.DataSource("rate", Map("rowsPerSecond" -> "5", "numPartitions" -> "1"), streaming = true)
      val u = Rel.DataSource("rate", Map("numPartitions" -> "1", "rowsPerSecond" -> "5"), streaming = true)
      assertTrue(
        RelCodec.parse(RelCodec.render(t)) == Right(t),
        RelCodec.render(t) == RelCodec.render(u), // map insertion order quotiented away
      )
    },
    test("data source with a raw schema DDL round-trips (wire schema preserved verbatim)") {
      val t = Rel.DataSource(
        "json",
        Map("path" -> "/data/in"),
        streaming = true,
        schema = List("id" -> ColType.I64, "amount" -> ColType.Unknown),
        schemaDdl = Some("id BIGINT, amount DECIMAL(10,2)"),
      )
      assertTrue(
        RelCodec.parse(RelCodec.render(t)) == Right(t),
        // the DECIMAL the ColType list lost is still exact in the round-tripped DDL
        RelCodec.parse(RelCodec.render(t)).toOption.flatMap(_.asInstanceOf[Rel.DataSource].schemaDdl)
          == Some("id BIGINT, amount DECIMAL(10,2)"),
      )
    },
    test("malformed input fails with the offending form, not an exception") {
      assertTrue(
        RelCodec.parse("(banana 1 2)").isLeft,
        RelCodec.parse("(project (read a batch) (col x)").isLeft, // unclosed
        RelCodec.parse("").isLeft,
      )
    },
  )
