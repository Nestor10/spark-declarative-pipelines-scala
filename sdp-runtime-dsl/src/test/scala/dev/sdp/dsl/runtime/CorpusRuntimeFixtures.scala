package dev.sdp.dsl.runtime

import scala.language.implicitConversions // InlineDf → Df (createDataFrame without .toDF)

import dev.sdp.core.GraphFragment
import dev.sdp.dsl.runtime.*
import dev.sdp.dsl.runtime.functions.*

/** Corpus of flow bodies built with the RUNTIME builder, one per construct
  * exercised by `dev.sdp.dsl.FlowLanguageSpec` and `WarehouseDogfoodSpec`.
  * Paired body-for-body with [[CorpusMacroFixtures]] (only the import differs);
  * `EquivalenceSpec` asserts `RelCodec.render` equality per construct.
  *
  * Each `val` name matches a key in `EquivalenceSpec.corpus`; the same body
  * appears in CorpusMacroFixtures under the same name.
  */
object CorpusRuntimeFixtures:

  // --- FlowLanguageSpec: extraction fidelity -------------------------

  val sourceOptionWhereSelect: GraphFragment = streamingTable("silver") {
    stream
      .source("rate")
      .option("rowsPerSecond", "1")
      .where(col("value") > lit(0L))
      .select(col("value"), (col("value") * lit(2)).as("doubled"))
  }

  val valIntermediates: GraphFragment = streamingTable("gold") {
    val threshold = lit(10)
    val base      = stream.table("silver").where(col("value") > threshold)
    base.select(col("value"))
  }

  val inlineToDf: GraphFragment = materializedView("regions") {
    spark.createDataFrame(Seq((1, "North"), (2, "South"))).toDF("id", "name")
  }

  val inlineMixed: GraphFragment = materializedView("mixed") {
    spark.createDataFrame(Seq((1, 1.5, "a"), (2L, null, "b")))
  }

  val inlineSingleColumn: GraphFragment = materializedView("ids") {
    spark.createDataFrame(Seq(1L, 2L, 3L)).toDF("id")
  }

  val groupAggJoinOrderLimit: GraphFragment = materializedView("report") {
    val left  = read.table("facts")
    val right = read.table("dims")
    left
      .join(right)(col("facts.id") === col("dims.id"))
      .groupBy(col("dims.kind"))
      .agg(fn("count", col("facts.id")).as("n"))
      .orderBy(col("n").desc)
      .limit(10)
  }

  val castSampleHint: GraphFragment = streamingTable("t1") {
    stream
      .table("src")
      .select(col("id").cast(string).as("id_str"))
      .sample(0.25, 42L)
      .hint("broadcast")
  }

  val repartitionNaStat: GraphFragment = materializedView("stats") {
    read
      .range(0L, 100L)
      .repartition(4)
      .dropNa("id")
      .fillNa(lit(0L), "id")
      .describe("id")
  }

  val unpivotObserve: GraphFragment = materializedView("melted") {
    read
      .range(0L, 10L)
      .observe("input_count", fn("count", col("id")).as("n"))
      .unpivot(col("id"))("metric", "value")
  }

  val exprStarWindow: GraphFragment = materializedView("ranked") {
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

  val containerAccess: GraphFragment = materializedView("containers") {
    read.range(0L, 1L).select(
      col("m").getItem(lit("k")).as("v"),
      col("s").getField("inner").as("f"),
    )
  }

  // lambda param names match the macro's author-source names (item / l,r) via
  // the named lam/lam2 runtime escape so LamVar names render identically.
  val lambdasNamed: GraphFragment = materializedView("hof") {
    read.range(0L, 1L).select(
      fn("transform", col("xs"), lam("item")(item => item * lit(2))).as("doubled"),
      fn("zip_with", col("a"), col("b"), lam2("l", "r")((l, r) => l + r)).as("sums"),
    )
  }

  val subqueries: GraphFragment = materializedView("subq") {
    read
      .range(0L, 100L)
      .where(exists(read.table("flags").where(col("active") === lit(true))))
      .where(col("id").in(read.table("allowed_ids")))
      .select(
        col("id"),
        scalar(read.table("stats").groupBy().agg(fn("max", col("v")))).as("max_v"),
      )
  }

  val tastyRoundTrip: GraphFragment = streamingTable("rt") {
    stream.table("src").where(col("a") && col("b")).limit(3)
  }

  // spark facade family (each body is a single leaf/op)
  val facadeTable: GraphFragment      = materializedView("t") { spark.table("orders") }
  val facadeSql: GraphFragment        = materializedView("q") { spark.sql("SELECT 1 AS one") }
  val facadeRange1: GraphFragment     = materializedView("r1") { spark.range(10) }
  val facadeRange3: GraphFragment     = materializedView("r3") { spark.range(5, 50, 5) }
  val facadeReadTable: GraphFragment  = materializedView("rt") { spark.read.table("dim") }
  val facadeReadStream: GraphFragment = streamingTable("st") { spark.readStream.table("orders") }
  val facadeStreamSource: GraphFragment = streamingTable("ss") {
    spark.readStream.format("rate").option("rowsPerSecond", "1").load()
  }
  val facadeBatchSource: GraphFragment = materializedView("bs") {
    spark.read.format("csv").load("/data/seed.csv")
  }

  val guideExample: GraphFragment = materializedView("daily_orders_by_state") {
    spark.table("customer_orders")
      .groupBy("state", "order_date")
      .count()
      .withColumnRenamed("count", "order_count")
  }

  // sugar: string columns, filter/sort aliases, isin, na, stat
  val sugarStringsSortFilter: GraphFragment = materializedView("a") {
    spark.table("t").filter(col("x") > lit(0)).select("x", "y").sort("y")
  }
  val sugarIsin: GraphFragment = materializedView("b") {
    spark.table("t").where(col("state").isin(lit("CA"), lit("NY")))
  }
  val sugarNaFill: GraphFragment = materializedView("c") {
    spark.table("t").na.fill(lit(0L), Seq("a", "b"))
  }
  val sugarStatCorr: GraphFragment = materializedView("d") {
    spark.table("t").stat.corr("x", "y")
  }

  val withColumnsMaps: GraphFragment = materializedView("m") {
    spark.table("t")
      .withColumns(Map("a" -> lit(1), "b" -> (col("x") * lit(2))))
      .withColumnsRenamed(Map("a" -> "alpha", "b" -> "beta"))
  }

  val functionsFacade: GraphFragment = materializedView("report") {
    spark.table("orders")
      .groupBy(col("state"))
      .agg(count(col("id")).as("n"), sum(col("amount")).as("total"), mean(col("amount")))
      .withColumn("flag", when(col("n") > lit(10), lit("hot")).when(col("n") > lit(5), lit("warm")).otherwise(lit("cold")))
      .withColumn("power", pow(col("total"), lit(2.0)))
      .withColumn("joined", concat_ws(",", col("state"), col("flag")))
  }

  val hofsNoWrapper: GraphFragment = materializedView("hof") {
    spark.table("t").select(
      transform(col("xs"), x => x * lit(2)).as("doubled"),
      zip_with(col("a"), col("b"), (l, r) => l + r).as("sums"),
      lag(col("v"), 1).over(window.partitionBy(col("k")).orderBy(col("ts"))).as("prev"),
    )
  }

  val ddlSchema: GraphFragment = streamingTable("rates") {
    spark.readStream
      .format("rate")
      .schema("timestamp TIMESTAMP, value BIGINT, note DECIMAL(10,2)")
      .load()
      .select(col("value"), col("timestamp").as("seen_at"))
  }

  // --- WarehouseDogfoodSpec extra constructs -------------------------

  val withSchemaSource: GraphFragment = streamingTable("raw_events") {
    stream
      .source("rate")
      .option("rowsPerSecond", "50")
      .withSchema(field("timestamp", timestamp), field("value", long))
      .select(col("timestamp"), col("value").as("event_id"))
  }

  val crossModulo: GraphFragment = streamingTable("orders") {
    val typed = stream.table("raw_events").withColumn("order_id", col("event_id") % lit(100000L))
    typed.withColumn("amount", (col("event_id") % lit(500L)) + lit(1L))
  }

  val booleanAnd: GraphFragment = streamingTable("valid_orders") {
    stream.table("orders").where(col("amount") > lit(0L) && col("order_id") =!= lit(0L))
  }

  val dropDuplicatesOne: GraphFragment = streamingTable("deduped_orders") {
    stream.table("valid_orders").dropDuplicates("order_id")
  }

  val joinDrop: GraphFragment = streamingTable("bucketed_orders") {
    val withBucket = stream.table("deduped_orders").withColumn("bucket_id", col("amount") % lit(10L))
    withBucket
      .join(read.table("dim_buckets"))(col("bucket_id") === col("bucket_id"))
      .drop("event_id")
  }

  val unionDistinct: GraphFragment = materializedView("extreme_buckets") {
    read.table("top_buckets").union(read.table("bottom_buckets")).distinct
  }

  val aliasSelect: GraphFragment = materializedView("quiet_days") {
    val weekend = read.table("dim_calendar").where(col("day_of_week") >= lit(5L))
    weekend.alias("weekend_days").select(col("day_index"))
  }

  val offsetTail: GraphFragment = materializedView("audit_sample") {
    read.table("bucketed_orders").offset(10).tail(100)
  }

  val orderByAsc: GraphFragment = materializedView("bottom_buckets") {
    read.table("bucket_revenue").orderBy(col("revenue").asc).limit(3)
  }

  // --- typed columns (cols[S]) — render-identical to plain col(...) ---
  val typedCols: GraphFragment = materializedView("typed") {
    type Orders = (order_id: Long, amount: Long, state: String)
    val c = cols[Orders]
    spark.table("bronze.orders").where(c.amount > lit(0L)).select(c.order_id, c.state)
  }
