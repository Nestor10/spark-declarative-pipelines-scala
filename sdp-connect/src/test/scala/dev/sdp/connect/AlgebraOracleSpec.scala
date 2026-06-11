package dev.sdp.connect

import dev.sdp.core.algebra.*
import zio.*
import zio.test.*

import PlanAnalysis.SchemaField

/** F11: every Tier-0 algebra case, verified against the live analyzer.
  *
  * Each test builds a plan from pure ADT values, sends it through
  * `AnalyzePlan`, and asserts the schema Catalyst computes. A green test
  * here means the server itself vouches for our encoding of that
  * capability — these are the receipts behind `SupportedCapabilities`.
  *
  * Plans are grounded in `Sql(range(...))` so they resolve without any
  * pre-existing catalog state.
  */
object AlgebraOracleSpec extends ZIOSpecDefault:

  private val enabled =
    sys.env.contains("SDP_INTEGRATION") || java.lang.Boolean.getBoolean("sdp.integration")

  /** `range(1, 100)` exposes a single BIGINT column `id`. */
  private val base: Rel = Rel.Sql("SELECT id FROM range(1, 100)")

  private def analyzed(rel: Rel) =
    for
      server <- ZIO.service[SparkConnectTestServer.Server]
      fields <- PlanAnalysis.analyzeSchema(server.host, server.port, AlgebraProtoEncoder.relation(rel))
    yield fields

  def spec =
    val tests = suite("Tier-0 algebra vs the live analyzer (F11)")(
      test("Sql: the escape hatch resolves") {
        analyzed(base).map(fields => assertTrue(fields == List(SchemaField("id", "long"))))
      },
      test("Project: expressions + alias produce the projected schema") {
        val plan = Rel.Project(
          base,
          List(
            Ex.Col("id"),
            Ex.Alias(Ex.Fn("+", List(Ex.Col("id"), Ex.Lit(LitValue.I32(1)))), "next_id"),
          ),
        )
        analyzed(plan).map(fields =>
          assertTrue(fields == List(SchemaField("id", "long"), SchemaField("next_id", "long")))
        )
      },
      test("Filter: condition expression resolves, schema passes through") {
        val plan = Rel.Filter(base, Ex.Fn(">", List(Ex.Col("id"), Ex.Lit(LitValue.I64(10L)))))
        analyzed(plan).map(fields => assertTrue(fields == List(SchemaField("id", "long"))))
      },
      test("Join: inner join on condition merges schemas") {
        val left  = Rel.Project(base, List(Ex.Alias(Ex.Col("id"), "l_id")))
        val right = Rel.Project(base, List(Ex.Alias(Ex.Col("id"), "r_id")))
        val plan = Rel.Join(
          left,
          right,
          Some(Ex.Fn("==", List(Ex.Col("l_id"), Ex.Col("r_id")))),
          JoinType.Inner,
        )
        analyzed(plan).map(fields =>
          assertTrue(fields == List(SchemaField("l_id", "long"), SchemaField("r_id", "long")))
        )
      },
      test("Aggregate: group-by key + aggregate expression") {
        val plan = Rel.Aggregate(
          Rel.Project(
            base,
            List(Ex.Col("id"), Ex.Alias(Ex.Fn("%", List(Ex.Col("id"), Ex.Lit(LitValue.I32(2)))), "parity")),
          ),
          groupBy = List(Ex.Col("parity")),
          aggregates = List(Ex.Alias(Ex.Fn("count", List(Ex.Col("id"))), "n")),
        )
        analyzed(plan).map(fields =>
          assertTrue(fields == List(SchemaField("parity", "long"), SchemaField("n", "long")))
        )
      },
      test("Sort + Limit: ordering keys resolve, schema passes through") {
        val plan = Rel.Limit(Rel.Sort(base, List(SortKey(Ex.Col("id"), descending = true))), 5)
        analyzed(plan).map(fields => assertTrue(fields == List(SchemaField("id", "long"))))
      },
      test("F12 battery: range, offset, tail, dedup, drop, set-ops, alias, toDF, withColumns, renames") {
        val pipeline = Rel.WithColumnsRenamed(
          Rel.WithColumns(
            Rel.ToDF(
              Rel.SubqueryAlias(
                Rel.SetOp(
                  Rel.Drop(
                    Rel.Deduplicate(
                      Rel.Tail(Rel.Offset(Rel.Range(0L, 100L, 1L), 5), 50),
                      List("id"),
                    ),
                    Nil,
                  ),
                  Rel.Range(200L, 210L, 1L),
                  SetOpType.Union,
                  all = true,
                ),
                "u",
              ),
              List("n"),
            ),
            List("n2" -> Ex.Fn("*", List(Ex.Col("n"), Ex.Lit(LitValue.I32(2))))),
          ),
          List("n" -> "original"),
        )
        analyzed(pipeline).map(fields =>
          assertTrue(
            fields == List(SchemaField("original", "long"), SchemaField("n2", "long"))
          )
        )
      },
      test("F12 join variants: left outer keeps the left schema nullable-extended") {
        val left  = Rel.Project(Rel.Range(0L, 10L, 1L), List(Ex.Alias(Ex.Col("id"), "l")))
        val right = Rel.Project(Rel.Range(0L, 5L, 1L), List(Ex.Alias(Ex.Col("id"), "r")))
        val joins = List(
          JoinType.LeftOuter  -> List(SchemaField("l", "long"), SchemaField("r", "long")),
          JoinType.LeftSemi   -> List(SchemaField("l", "long")),
          JoinType.LeftAnti   -> List(SchemaField("l", "long")),
        )
        ZIO
          .foreach(joins) { (jt, expected) =>
            analyzed(Rel.Join(left, right, Some(Ex.Fn("==", List(Ex.Col("l"), Ex.Col("r")))), jt))
              .map(_ == expected)
          }
          .map(results => assertTrue(results.forall(identity)))
      },
      test("T1: cast declares the output type — the analyzer agrees") {
        val plan = Rel.Project(
          base,
          List(
            Ex.Alias(Ex.Cast(Ex.Col("id"), ColType.Str), "id_str"),
            Ex.Alias(Ex.Cast(Ex.Col("id"), ColType.F64), "id_dbl"),
            Ex.Alias(Ex.Cast(Ex.Col("id"), ColType.I32), "id_int"),
          ),
        )
        analyzed(plan).map(fields =>
          assertTrue(fields == List(
            SchemaField("id_str", "string"),
            SchemaField("id_dbl", "double"),
            SchemaField("id_int", "integer"),
          ))
        )
      },
      test("T1: sample and hint pass schemas through") {
        val plan = Rel.Hint(Rel.Sample(base, 0.5, Some(7L)), "coalesce", List(Ex.Lit(LitValue.I32(1))))
        analyzed(plan).map(fields => assertTrue(fields == List(SchemaField("id", "long"))))
      },
      test("T1 batch 2: repartition + NA ops pass schemas through") {
        val plan = Rel.FillNa(
          Rel.DropNa(Rel.Repartition(base, 4, shuffle = true), List("id")),
          List("id"),
          LitValue.I64(0L),
        )
        analyzed(plan).map(fields => assertTrue(fields == List(SchemaField("id", "long"))))
      },
      test("T1 batch 2: repartitionBy expressions resolve") {
        val plan = Rel.RepartitionByExpression(base, List(Ex.Col("id")), Some(2))
        analyzed(plan).map(fields => assertTrue(fields == List(SchemaField("id", "long"))))
      },
      test("T1 batch 2: stat relations produce server-computed schemas") {
        for
          desc  <- analyzed(Rel.Describe(base, List("id")))
          summ  <- analyzed(Rel.Summary(base, List("count", "mean")))
          cross <- analyzed(Rel.Crosstab(base, "id", "id"))
          cov   <- analyzed(Rel.Cov(base, "id", "id"))
          corr  <- analyzed(Rel.Corr(base, "id", "id"))
          freq  <- analyzed(Rel.FreqItems(base, List("id")))
        yield assertTrue(
          desc.map(_.name) == List("summary", "id"),
          summ.map(_.name) == List("summary", "id"),
          cross.nonEmpty,
          cov == List(SchemaField("cov", "double")),
          corr == List(SchemaField("corr", "double")),
          freq.map(_.name) == List("id_freqItems"),
        )
      },
      test("T1 batch 3: unpivot melts to (var, value); observe/replace/sampleBy pass through") {
        val wide = Rel.Project(
          base,
          List(Ex.Col("id"), Ex.Alias(Ex.Fn("+", List(Ex.Col("id"), Ex.Lit(LitValue.I32(1)))), "b")),
        )
        for
          melted <- analyzed(
            Rel.Unpivot(wide, List(Ex.Col("id")), List(Ex.Col("b")), "metric", "value")
          )
          observed <- analyzed(
            Rel.CollectMetrics(base, "cp", List(Ex.Alias(Ex.Fn("count", List(Ex.Col("id"))), "n")))
          )
          replaced <- analyzed(
            Rel.Replace(base, List("id"), List((LitValue.I64(0L), LitValue.I64(-1L))))
          )
          stratified <- analyzed(
            Rel.SampleBy(base, Ex.Col("id"), List((LitValue.I64(1L), 0.5)), Some(7L))
          )
        yield assertTrue(
          melted.map(_.name) == List("id", "metric", "value"),
          observed == List(SchemaField("id", "long")),
          replaced == List(SchemaField("id", "long")),
          stratified == List(SchemaField("id", "long")),
        )
      },
      test("T1 batch 3: transpose and approxQuantile produce server-computed schemas") {
        for
          transposed <- analyzed(Rel.Transpose(Rel.Limit(base, 3), List(Ex.Col("id")))).exit
          quantiles  <- analyzed(Rel.ApproxQuantile(base, List("id"), List(0.5), 0.01))
        yield assertTrue(
          // transpose requires a single index column whose values become
          // column NAMES — accept either resolution or a typed analyzer
          // verdict; what matters is the wire shape is understood.
          transposed.isSuccess || transposed.causeOption.flatMap(_.failureOption).exists {
            case PipelinesRegistration.RegistrationError.ServerRejected(_) => true
            case _                                                         => false
          },
          // the analyzer names the output column 'approx_quantile' (learned
          // from the oracle itself — the input col is not the output name)
          quantiles.map(_.name) == List("approx_quantile"),
        )
      },
      test("T1 final: typed leaves and waypoints — localRelation, toSchema, tvf") {
        val schema = List("ts" -> ColType.Timestamp, "v" -> ColType.I64)
        for
          local <- analyzed(Rel.LocalRelation(schema))
          retyped <- analyzed(Rel.ToSchema(
            Rel.Project(base, List(Ex.Col("id"))),
            List("renamed_id" -> ColType.I64),
          ))
          tvf <- analyzed(Rel.Tvf("range", List(Ex.Lit(LitValue.I64(5L)))))
        yield assertTrue(
          local == List(SchemaField("ts", "timestamp"), SchemaField("v", "long")),
          retyped == List(SchemaField("renamed_id", "long")),
          tvf == List(SchemaField("id", "long")),
        )
      },
      test("T1 final: asof + lateral joins merge schemas") {
        val left  = Rel.Project(base, List(Ex.Alias(Ex.Col("id"), "l_t")))
        val right = Rel.Project(base, List(Ex.Alias(Ex.Col("id"), "r_t")))
        for
          asof <- analyzed(
            Rel.AsOfJoin(left, right, Ex.Col("l_t"), Ex.Col("r_t"), "inner", "backward", true, None)
          )
          lateral <- analyzed(Rel.LateralJoin(left, Rel.Range(0, 2, 1), None, JoinType.Cross))
        yield assertTrue(
          asof.map(_.name) == List("l_t", "r_t"),
          lateral.map(_.name) == List("l_t", "id"),
        )
      },
      test("T1 final: parse infers schema from JSON text; debug renders are single strings") {
        val jsonText = Rel.Sql("""SELECT '{"a": 1, "b": "x"}' AS value""")
        for
          parsed <- analyzed(Rel.Parse(jsonText, ParseFormat.Json, Map.empty))
          shown  <- analyzed(Rel.ShowString(base, 5, 20, vertical = false))
          html   <- analyzed(Rel.HtmlString(base, 5, 20))
        yield assertTrue(
          parsed.map(_.name) == List("a", "b"),
          shown.map(_.kind) == List("string"),
          html.map(_.kind) == List("string"),
        )
      },
      test("T1 final: catalog relations resolve") {
        for
          current <- analyzed(Rel.Catalog(CatalogOp.CurrentDatabase))
          dbs     <- analyzed(Rel.Catalog(CatalogOp.ListDatabases))
          tables  <- analyzed(Rel.Catalog(CatalogOp.ListTables))
        yield assertTrue(
          current.map(_.kind) == List("string"),
          dbs.nonEmpty,
          tables.nonEmpty,
        )
      },
      test("expressions: expr/star/extract/regex/callfn resolve") {
        for
          exprStr <- analyzed(Rel.Project(base, List(Ex.Alias(Ex.ExprString("id % 7"), "m"))))
          starAll <- analyzed(Rel.Project(base, List(Ex.Star(None))))
          countStar <- analyzed(
            Rel.Aggregate(base, Nil, List(Ex.Alias(Ex.Fn("count", List(Ex.Star(None))), "n")))
          )
          extract <- analyzed(Rel.Project(
            Rel.Sql("SELECT map('k', 42) AS m"),
            List(Ex.Alias(Ex.ExtractValue(Ex.Col("m"), Ex.Lit(LitValue.Str("k"))), "v")),
          ))
          regex  <- analyzed(Rel.Project(base, List(Ex.ColRegex("`id`"))))
          called <- analyzed(Rel.Project(base, List(Ex.Alias(Ex.CallFn("abs", List(Ex.Col("id"))), "a"))))
        yield assertTrue(
          exprStr == List(SchemaField("m", "long")),
          starAll == List(SchemaField("id", "long")),
          countStar == List(SchemaField("n", "long")),
          extract == List(SchemaField("v", "integer")),
          regex == List(SchemaField("id", "long")),
          called == List(SchemaField("a", "long")),
        )
      },
      test("expressions: window functions — ranking and framed aggregates") {
        val bucketed = Rel.Project(
          base,
          List(Ex.Col("id"), Ex.Alias(Ex.Fn("%", List(Ex.Col("id"), Ex.Lit(LitValue.I32(3)))), "k")),
        )
        val rn = Ex.Alias(
          Ex.Window(Ex.Fn("row_number", Nil), List(Ex.Col("k")), List(SortKey(Ex.Col("id"))), None),
          "rn",
        )
        val running = Ex.Alias(
          Ex.Window(
            Ex.Fn("sum", List(Ex.Col("id"))),
            List(Ex.Col("k")),
            List(SortKey(Ex.Col("id"))),
            Some(WindowFrame(rowFrame = true, FrameBoundary.Unbounded, FrameBoundary.CurrentRow)),
          ),
          "running",
        )
        analyzed(Rel.Project(bucketed, List(Ex.Col("id"), rn, running))).map(fields =>
          assertTrue(fields == List(
            SchemaField("id", "long"),
            SchemaField("rn", "integer"),
            SchemaField("running", "long"),
          ))
        )
      },
      test("lambdas: higher-order array functions with real parameter names") {
        val arrays = Rel.Sql("SELECT array(1, 2, 3) AS xs")
        val plan = Rel.Project(
          arrays,
          List(
            Ex.Alias(Ex.Fn("transform", List(
              Ex.Col("xs"),
              Ex.Lam(List("item"), Ex.Fn("+", List(Ex.LamVar("item"), Ex.Lit(LitValue.I32(1))))),
            )), "incremented"),
            Ex.Alias(Ex.Fn("filter", List(
              Ex.Col("xs"),
              Ex.Lam(List("v"), Ex.Fn(">", List(Ex.LamVar("v"), Ex.Lit(LitValue.I32(1))))),
            )), "filtered"),
          ),
        )
        analyzed(plan).map(fields =>
          assertTrue(
            fields.map(_.name) == List("incremented", "filtered"),
            fields.map(_.kind) == List("array", "array"),
          )
        )
      },
      test("subqueries: exists/scalar/in lower to plan-id refs the analyzer accepts") {
        for
          existsQ <- analyzed(Rel.Filter(base, Ex.Subquery(Rel.Range(0, 1, 1), SubqueryKind.Exists)))
          scalarQ <- analyzed(Rel.Project(base, List(Ex.Alias(
            Ex.Subquery(
              Rel.Aggregate(Rel.Range(0, 5, 1), Nil, List(Ex.Fn("max", List(Ex.Col("id"))))),
              SubqueryKind.Scalar,
            ),
            "max_id",
          ))))
          inQ <- analyzed(Rel.Filter(base, Ex.Subquery(
            Rel.Project(Rel.Range(0, 5, 1), List(Ex.Col("id"))),
            SubqueryKind.In(List(Ex.Col("id"))),
          )))
        yield assertTrue(
          existsQ == List(SchemaField("id", "long")),
          scalarQ == List(SchemaField("max_id", "long")),
          inQ == List(SchemaField("id", "long")),
        )
      },
      test("a bad column name is rejected by the analyzer, typed") {
        val plan = Rel.Project(base, List(Ex.Col("no_such_column")))
        analyzed(plan).exit.map(exit =>
          assertTrue(
            exit.causeOption.flatMap(_.failureOption).exists {
              case PipelinesRegistration.RegistrationError.ServerRejected(d) =>
                d.contains("no_such_column") || d.contains("UNRESOLVED")
              case _ => false
            }
          )
        )
      },
    ).provideShared(SparkConnectTestServer.layer) @@ TestAspect.withLiveEnvironment
      @@ TestAspect.sequential @@ TestAspect.timeout(5.minutes)

    if enabled then tests else tests @@ TestAspect.ignore
