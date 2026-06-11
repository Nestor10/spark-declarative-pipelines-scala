package dev.sdp.dsl

import scala.compiletime.testing.typeCheckErrors

import dev.sdp.app.{GraphValidation, ManifestAssembly}
import dev.sdp.core.*
import dev.sdp.core.algebra.*
import dev.sdp.dsl.functions.* // the function library (count/sum/when/transform/…) — disjoint from dev.sdp.dsl
import zio.test.*

/** F11c: the fluent flow language, end to end — extraction fidelity, `val`
  * ergonomics, compile-time rejection, and the payoff: structural validation
  * over what flows *actually compute*.
  */
object FlowLanguageSpec extends ZIOSpecDefault:

  def spec = suite("flow language (F11c)")(
    suite("extraction fidelity")(
      test("source → option → where → select extracts the exact tree") {
        val fragment = streamingTableFrom("silver") {
          stream
            .source("rate")
            .option("rowsPerSecond", "1")
            .where(col("value") > lit(0L))
            .select(col("value"), (col("value") * lit(2)).as("doubled"))
        }
        assertTrue(
          fragment.flows == List(Flow(
            "silver",
            "silver",
            Rel.Project(
              Rel.Filter(
                Rel.DataSource("rate", Map("rowsPerSecond" -> "1"), streaming = true),
                Ex.Fn(">", List(Ex.Col("value"), Ex.Lit(LitValue.I64(0L)))),
              ),
              List(
                Ex.Col("value"),
                Ex.Alias(Ex.Fn("*", List(Ex.Col("value"), Ex.Lit(LitValue.I32(2)))), "doubled"),
              ),
            ),
          ))
        )
      },
      test("val intermediates participate in extraction") {
        val fragment = streamingTableFrom("gold") {
          val threshold = lit(10)
          val base      = stream.table("silver").where(col("value") > threshold)
          base.select(col("value"))
        }
        assertTrue(
          fragment.flows.head.relation == Rel.Project(
            Rel.Filter(
              Rel.NamedTable("silver", streaming = true),
              Ex.Fn(">", List(Ex.Col("value"), Ex.Lit(LitValue.I32(10)))),
            ),
            List(Ex.Col("value")),
          )
        )
      },
      test("inline data: spark.createDataFrame(Seq(...)).toDF(...) extracts to LocalData") {
        val fragment = materializedViewFrom("regions") {
          spark.createDataFrame(Seq((1, "North"), (2, "South"))).toDF("id", "name")
        }
        assertTrue(
          fragment.flows.head.relation == Rel.LocalData(
            List("id" -> ColType.I32, "name" -> ColType.Str),
            List(
              List(LitValue.I32(1), LitValue.Str("North")),
              List(LitValue.I32(2), LitValue.Str("South")),
            ),
          )
        )
      },
      test("inline data: numeric columns widen, nulls take the column type, names default to _N") {
        val fragment = materializedViewFrom("mixed") {
          spark.createDataFrame(Seq((1, 1.5, "a"), (2L, null, "b")))
        }
        assertTrue(
          fragment.flows.head.relation == Rel.LocalData(
            // col1: I32 ∪ I64 → I64; col2: F64 with a null; col3: Str
            List("_1" -> ColType.I64, "_2" -> ColType.F64, "_3" -> ColType.Str),
            List(
              List(LitValue.I32(1), LitValue.F64(1.5), LitValue.Str("a")),
              List(LitValue.I64(2L), LitValue.Null, LitValue.Str("b")),
            ),
          )
        )
      },
      test("inline data: a single-column table uses bare literals (no tuple)") {
        val fragment = materializedViewFrom("ids") {
          spark.createDataFrame(Seq(1L, 2L, 3L)).toDF("id")
        }
        assertTrue(
          fragment.flows.head.relation == Rel.LocalData(
            List("id" -> ColType.I64),
            List(List(LitValue.I64(1L)), List(LitValue.I64(2L)), List(LitValue.I64(3L))),
          )
        )
      },
      test("groupBy.agg, join, orderBy and limit extract") {
        val fragment = materializedViewFrom("report") {
          val left  = read.table("facts")
          val right = read.table("dims")
          left
            .join(right)(col("facts.id") === col("dims.id"))
            .groupBy(col("dims.kind"))
            .agg(fn("count", col("facts.id")).as("n"))
            .orderBy(col("n").desc)
            .limit(10)
        }
        val expected = Rel.Limit(
          Rel.Sort(
            Rel.Aggregate(
              Rel.Join(
                Rel.NamedTable("facts", false),
                Rel.NamedTable("dims", false),
                Some(Ex.Fn("==", List(Ex.Col("facts.id"), Ex.Col("dims.id")))),
                JoinType.Inner,
              ),
              List(Ex.Col("dims.kind")),
              List(Ex.Alias(Ex.Fn("count", List(Ex.Col("facts.id"))), "n")),
            ),
            List(SortKey(Ex.Col("n"), descending = true)),
          ),
          10,
        )
        assertTrue(fragment.flows.head.relation == expected)
      },
      test("T1: cast, sample and hint extract") {
        val fragment = streamingTableFrom("t1") {
          stream
            .table("src")
            .select(col("id").cast(string).as("id_str"))
            .sample(0.25, 42L)
            .hint("broadcast")
        }
        assertTrue(
          fragment.flows.head.relation == Rel.Hint(
            Rel.Sample(
              Rel.Project(
                Rel.NamedTable("src", streaming = true),
                List(Ex.Alias(Ex.Cast(Ex.Col("id"), ColType.Str), "id_str")),
              ),
              0.25,
              Some(42L),
            ),
            "broadcast",
            Nil,
          )
        )
      },
      test("T1 batch 2: repartition, NA ops and stat relations extract") {
        val fragment = materializedViewFrom("stats") {
          read
            .range(0L, 100L)
            .repartition(4)
            .dropNa("id")
            .fillNa(lit(0L), "id")
            .describe("id")
        }
        assertTrue(
          fragment.flows.head.relation == Rel.Describe(
            Rel.FillNa(
              Rel.DropNa(
                Rel.Repartition(Rel.Range(0L, 100L, 1L), 4, shuffle = true),
                List("id"),
              ),
              List("id"),
              LitValue.I64(0L),
            ),
            List("id"),
          )
        )
      },
      test("T1 batch 3: unpivot, transpose, replaceValues and observe extract") {
        val fragment = materializedViewFrom("melted") {
          read
            .range(0L, 10L)
            .observe("input_count", fn("count", col("id")).as("n"))
            .unpivot(col("id"))("metric", "value")
        }
        assertTrue(
          fragment.flows.head.relation == Rel.Unpivot(
            Rel.CollectMetrics(
              Rel.Range(0L, 10L, 1L),
              "input_count",
              List(Ex.Alias(Ex.Fn("count", List(Ex.Col("id"))), "n")),
            ),
            List(Ex.Col("id")),
            Nil,
            "metric",
            "value",
          )
        )
      },
      test("expressions: expr, star, getItem/getField and windows extract") {
        val fragment = materializedViewFrom("ranked") {
          read
            .range(0L, 100L)
            .withColumn("bucket", expr("id % 7"))
            .groupBy(col("bucket"))
            .agg(fn("count", star).as("n"))
            .select(
              col("bucket"),
              col("n"),
              fn("row_number").over(window.partitionBy(col("bucket")).orderBy(col("n").desc)).as("rank"),
            )
        }
        val expected = Rel.Project(
          Rel.Aggregate(
            Rel.WithColumns(Rel.Range(0L, 100L, 1L), List("bucket" -> Ex.ExprString("id % 7"))),
            List(Ex.Col("bucket")),
            List(Ex.Alias(Ex.Fn("count", List(Ex.Star(None))), "n")),
          ),
          List(
            Ex.Col("bucket"),
            Ex.Col("n"),
            Ex.Alias(
              Ex.Window(
                Ex.Fn("row_number", Nil),
                List(Ex.Col("bucket")),
                List(SortKey(Ex.Col("n"), descending = true)),
                None,
              ),
              "rank",
            ),
          ),
        )
        assertTrue(fragment.flows.head.relation == expected)
      },
      test("expressions: container access extracts") {
        val fragment = materializedViewFrom("containers") {
          read.range(0L, 1L).select(
            col("m").getItem(lit("k")).as("v"),
            col("s").getField("inner").as("f"),
          )
        }
        assertTrue(
          fragment.flows.head.relation == Rel.Project(
            Rel.Range(0L, 1L, 1L),
            List(
              Ex.Alias(Ex.ExtractValue(Ex.Col("m"), Ex.Lit(LitValue.Str("k"))), "v"),
              Ex.Alias(Ex.ExtractValue(Ex.Col("s"), Ex.Lit(LitValue.Str("inner"))), "f"),
            ),
          )
        )
      },
      test("lambdas: a real Scala lambda extracts, preserving the author's parameter name") {
        val fragment = materializedViewFrom("hof") {
          read.range(0L, 1L).select(
            fn("transform", col("xs"), lam(item => item * lit(2))).as("doubled"),
            fn("zip_with", col("a"), col("b"), lam2((l, r) => l + r)).as("sums"),
          )
        }
        assertTrue(
          fragment.flows.head.relation == Rel.Project(
            Rel.Range(0L, 1L, 1L),
            List(
              Ex.Alias(Ex.Fn("transform", List(
                Ex.Col("xs"),
                Ex.Lam(List("item"), Ex.Fn("*", List(Ex.LamVar("item"), Ex.Lit(LitValue.I32(2))))),
              )), "doubled"),
              Ex.Alias(Ex.Fn("zip_with", List(
                Ex.Col("a"), Ex.Col("b"),
                Ex.Lam(List("l", "r"), Ex.Fn("+", List(Ex.LamVar("l"), Ex.LamVar("r")))),
              )), "sums"),
            ),
          )
        )
      },
      test("subqueries: exists, scalar and in embed relations in expression position") {
        val fragment = materializedViewFrom("subq") {
          read
            .range(0L, 100L)
            .where(exists(read.table("flags").where(col("active") === lit(true))))
            .where(col("id").in(read.table("allowed_ids")))
            .select(
              col("id"),
              scalar(read.table("stats").groupBy().agg(fn("max", col("v")))).as("max_v"),
            )
        }
        val rel = fragment.flows.head.relation
        // lineage sees THROUGH the subqueries:
        assertTrue(
          Flow.reads(rel) == Set("flags", "allowed_ids", "stats"),
          RelCodec.parse(RelCodec.render(rel)) == Right(rel),
        )
      },
      test("flow trees survive the TASTy embed round-trip") {
        val fragment = streamingTableFrom("rt") {
          stream.table("src").where(col("a") && col("b")).limit(3)
        }
        assertTrue(GraphFragment.parse(GraphFragment.render(fragment)) == Right(fragment))
      },
      test("the spark facade mirrors SparkSession: table/sql/range/read/readStream") {
        val t  = materializedViewFrom("t") { spark.table("orders") }
        val q  = materializedViewFrom("q") { spark.sql("SELECT 1 AS one") }
        val r1 = materializedViewFrom("r1") { spark.range(10) }
        val r3 = materializedViewFrom("r3") { spark.range(5, 50, 5) }
        val rt = materializedViewFrom("rt") { spark.read.table("dim") }
        val st = streamingTableFrom("st") { spark.readStream.table("orders") }
        val ss = streamingTableFrom("ss") {
          spark.readStream.format("rate").option("rowsPerSecond", "1").load()
        }
        val bs = materializedViewFrom("bs") { spark.read.format("csv").load("/data/seed.csv") }
        assertTrue(
          t.flows.head.relation == Rel.NamedTable("orders", streaming = false),
          q.flows.head.relation == Rel.Sql("SELECT 1 AS one"),
          r1.flows.head.relation == Rel.Range(0L, 10L, 1L),
          r3.flows.head.relation == Rel.Range(5L, 50L, 5L),
          rt.flows.head.relation == Rel.NamedTable("dim", streaming = false),
          st.flows.head.relation == Rel.NamedTable("orders", streaming = true),
          ss.flows.head.relation ==
            Rel.DataSource("rate", Map("rowsPerSecond" -> "1"), streaming = true),
          bs.flows.head.relation ==
            Rel.DataSource("csv", Map("path" -> "/data/seed.csv"), streaming = false),
        )
      },
      test("facade and pre-facade spellings extract identically") {
        val a = streamingTableFrom("a") { spark.readStream.table("src") }
        val b = streamingTableFrom("a") { stream.table("src") }
        assertTrue(a == b)
      },
      test("the SDP programming-guide example compiles body-for-body") {
        // verbatim from spark.apache.org declarative-pipelines guide, modulo
        // the dataset-name argument (we extract names, not function names)
        val daily = materializedView("daily_orders_by_state") {
          spark.table("customer_orders")
            .groupBy("state", "order_date")
            .count()
            .withColumnRenamed("count", "order_count")
        }
        assertTrue(
          daily.flows.head.relation == Rel.WithColumnsRenamed(
            Rel.Aggregate(
              Rel.NamedTable("customer_orders", streaming = false),
              List(Ex.Col("state"), Ex.Col("order_date")),
              List(Ex.Alias(Ex.Fn("count", List(Ex.Star(None))), "count")),
            ),
            List("count" -> "order_count"),
          )
        )
      },
      test("sugar: string columns, filter/sort aliases, isin, na, stat") {
        val a = materializedView("a") {
          spark.table("t").filter(col("x") > lit(0)).select("x", "y").sort("y")
        }
        val b = materializedView("b") {
          spark.table("t").where(col("state").isin(lit("CA"), lit("NY")))
        }
        val c = materializedView("c") { spark.table("t").na.fill(lit(0L), Seq("a", "b")) }
        val d = materializedView("d") { spark.table("t").stat.corr("x", "y") }
        assertTrue(
          a.flows.head.relation == Rel.Sort(
            Rel.Project(
              Rel.Filter(Rel.NamedTable("t", false), Ex.Fn(">", List(Ex.Col("x"), Ex.Lit(LitValue.I32(0))))),
              List(Ex.Col("x"), Ex.Col("y")),
            ),
            List(SortKey(Ex.Col("y"))),
          ),
          b.flows.head.relation == Rel.Filter(
            Rel.NamedTable("t", false),
            Ex.Fn("in", List(Ex.Col("state"), Ex.Lit(LitValue.Str("CA")), Ex.Lit(LitValue.Str("NY")))),
          ),
          c.flows.head.relation == Rel.FillNa(Rel.NamedTable("t", false), List("a", "b"), LitValue.I64(0L)),
          d.flows.head.relation == Rel.Corr(Rel.NamedTable("t", false), "x", "y"),
        )
      },
      test("sugar: withColumns(Map) and withColumnsRenamed(Map) keep source order") {
        val frag = materializedView("m") {
          spark.table("t")
            .withColumns(Map("a" -> lit(1), "b" -> (col("x") * lit(2))))
            .withColumnsRenamed(Map("a" -> "alpha", "b" -> "beta"))
        }
        assertTrue(
          frag.flows.head.relation == Rel.WithColumnsRenamed(
            Rel.WithColumns(
              Rel.NamedTable("t", false),
              List("a" -> Ex.Lit(LitValue.I32(1)), "b" -> Ex.Fn("*", List(Ex.Col("x"), Ex.Lit(LitValue.I32(2))))),
            ),
            List("a" -> "alpha", "b" -> "beta"),
          )
        )
      },
      test("the functions facade lowers Spark names to Ex.Fn") {
        val fragment = materializedViewFrom("report") {
          spark.table("orders")
            .groupBy(col("state"))
            .agg(count(col("id")).as("n"), sum(col("amount")).as("total"), mean(col("amount")))
            .withColumn("flag", when(col("n") > lit(10), lit("hot")).when(col("n") > lit(5), lit("warm")).otherwise(lit("cold")))
            .withColumn("power", pow(col("total"), lit(2.0)))
            .withColumn("joined", concat_ws(",", col("state"), col("flag")))
        }
        val agg = fragment.flows.head.relation match
          case Rel.WithColumns(Rel.Aggregate(_, _, aggs), cols) => Some((aggs, cols))
          case _                                                => None
        assertTrue(
          agg.get._1 == List(
            Ex.Alias(Ex.Fn("count", List(Ex.Col("id"))), "n"),
            Ex.Alias(Ex.Fn("sum", List(Ex.Col("amount"))), "total"),
            Ex.Fn("avg", List(Ex.Col("amount"))), // mean → avg, per Spark's own functions.scala
          ),
          agg.get._2 == List(
            "flag" -> Ex.Fn(
              "when",
              List(
                Ex.Fn(">", List(Ex.Col("n"), Ex.Lit(LitValue.I32(10)))), Ex.Lit(LitValue.Str("hot")),
                Ex.Fn(">", List(Ex.Col("n"), Ex.Lit(LitValue.I32(5)))), Ex.Lit(LitValue.Str("warm")),
                Ex.Lit(LitValue.Str("cold")),
              ),
            ),
            "power"  -> Ex.Fn("power", List(Ex.Col("total"), Ex.Lit(LitValue.F64(2.0)))), // pow → power
            "joined" -> Ex.Fn("concat_ws", List(Ex.Lit(LitValue.Str(",")), Ex.Col("state"), Ex.Col("flag"))),
          ),
        )
      },
      test("HOFs take real Scala lambdas — no lam() wrapper") {
        val fragment = materializedViewFrom("hof") {
          spark.table("t").select(
            transform(col("xs"), x => x * lit(2)).as("doubled"),
            zip_with(col("a"), col("b"), (l, r) => l + r).as("sums"),
            lag(col("v"), 1).over(window.partitionBy(col("k")).orderBy(col("ts"))).as("prev"),
          )
        }
        val Rel.Project(_, cols) = fragment.flows.head.relation: @unchecked
        assertTrue(
          cols(0) == Ex.Alias(
            Ex.Fn("transform", List(Ex.Col("xs"), Ex.Lam(List("x"), Ex.Fn("*", List(Ex.LamVar("x"), Ex.Lit(LitValue.I32(2))))))),
            "doubled",
          ),
          cols(1) == Ex.Alias(
            Ex.Fn("zip_with", List(Ex.Col("a"), Ex.Col("b"), Ex.Lam(List("l", "r"), Ex.Fn("+", List(Ex.LamVar("l"), Ex.LamVar("r")))))),
            "sums",
          ),
          cols(2) == Ex.Alias(
            Ex.Window(
              Ex.Fn("lag", List(Ex.Col("v"), Ex.Lit(LitValue.I32(1)))),
              List(Ex.Col("k")),
              List(SortKey(Ex.Col("ts"))),
              None,
            ),
            "prev",
          ),
        )
      },
      test("DDL schema strings parse at compile time onto the source leaf") {
        val fragment = streamingTableFrom("rates") {
          spark.readStream
            .format("rate")
            .schema("timestamp TIMESTAMP, value BIGINT, note DECIMAL(10,2)")
            .load()
            .select(col("value"), col("timestamp").as("seen_at"))
        }
        val expectedSource = Rel.DataSource(
          "rate",
          Map.empty,
          streaming = true,
          schema = List(
            "timestamp" -> ColType.Timestamp,
            "value"     -> ColType.I64,
            "note"      -> ColType.Unknown, // unchecked type: name known, type gradual
          ),
          // the RAW ddl is preserved verbatim for the wire — note DECIMAL(10,2)
          // survives here even though the parsed list above lost it to Unknown
          schemaDdl = Some("timestamp TIMESTAMP, value BIGINT, note DECIMAL(10,2)"),
        )
        assertTrue(
          fragment.flows.head.relation ==
            Rel.Project(expectedSource, List(Ex.Col("value"), Ex.Alias(Ex.Col("timestamp"), "seen_at")))
        )
      },
    ),
    suite("compile-time rejection")(
      test("unsupported constructs are positioned compile errors") {
        val errors = typeCheckErrors(
          """
          import dev.sdp.dsl.*
          val bad = streamingTableFrom("x") {
            stream.table("y").limit(scala.util.Random.nextInt())
          }
          """
        )
        assertTrue(errors.exists(_.message.contains("limit requires an integer literal")))
      },
      test("malformed DDL schema strings fail at compile time, positioned") {
        val errors = typeCheckErrors(
          """
          import dev.sdp.dsl.*
          val bad = streamingTableFrom("x") {
            spark.readStream.format("rate").schema("value BIGINT,, oops").load()
          }
          """
        )
        assertTrue(errors.exists(_.message.contains("invalid schema DDL")))
      },
      test("non-literal column names are rejected") {
        val errors = typeCheckErrors(
          """
          import dev.sdp.dsl.*
          val name: String = "dynamic"
          val bad = streamingTableFrom("x") { stream.table("y").select(col(name)) }
          """
        )
        assertTrue(errors.exists(_.message.contains("col name must be a constant string literal")))
      },
    ),
    suite("the payoff: validation over real flow bodies")(
      test("lineage is derived from what flows actually read") {
        val silver = streamingTableFrom("silver") {
          stream.source("rate").select(col("value"))
        }
        val gold = streamingTableFrom("gold") {
          stream.table("silver").where(col("value") > lit(0L))
        }
        for manifest <- ManifestAssembly.assemble(List(silver, gold))
        yield assertTrue(manifest.edges == List(DependencyEdge("silver", "gold")))
      },
      test("a cycle written in flow bodies fails at assembly, with the path") {
        val a = streamingTableFrom("a") { stream.table("b").select(col("x")) }
        val b = streamingTableFrom("b") { stream.table("a").select(col("x")) }
        for exit <- ManifestAssembly.assemble(List(a, b)).exit
        yield assertTrue(
          exit.causeOption.flatMap(_.failureOption).exists(_.contains(
            PipelineValidationError.CycleDetected(List("a", "b", "a"))
          ))
        )
      },
    ),
  ).provide(ManifestAssembly.live, GraphValidation.live)
