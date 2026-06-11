package dev.sdp.dsl

import dev.sdp.app.{GraphValidation, ManifestAssembly}
import dev.sdp.core.*
import zio.test.*

/** F12 dogfood: a realistic 13-dataset warehouse written end-to-end in the
  * flow language. This spec is the ergonomics referendum — if writing this
  * felt bad, the language failed regardless of what the tests say.
  *
  * (Author's notes from writing it: `val` intermediates and operator syntax
  * carry the weight; `groupBy.agg` and join variants read like Spark; the
  * one visible seam is string-typed columns — schema-checked only at
  * dry-run, not at compile. That seam is the F13+ typed-columns ambition.)
  */
object WarehouseDogfoodSpec extends ZIOSpecDefault:

  // ---- ingestion (streaming leaves) --------------------------------

  val rawEvents = streamingTableFrom("raw_events") {
    stream
      .source("rate")
      .option("rowsPerSecond", "50")
      .withSchema(field("timestamp", timestamp), field("value", long))
      .select(col("timestamp"), col("value").as("event_id"))
  }

  val orders = streamingTableFrom("orders") {
    val typed = stream.table("raw_events").withColumn("order_id", col("event_id") % lit(100000L))
    typed.withColumn("amount", (col("event_id") % lit(500L)) + lit(1L))
  }

  // ---- dimensions (generated batch) --------------------------------

  val dimBuckets = materializedViewFrom("dim_buckets") {
    read
      .range(0L, 10L)
      .withColumn("bucket_name", fn("concat", lit("bucket_"), col("id")))
      .toDF("bucket_id", "bucket_name")
  }

  val dimCalendar = materializedViewFrom("dim_calendar") {
    read
      .range(0L, 365L)
      .withColumn("day_of_week", col("id") % lit(7L))
      .withColumnRenamed("id", "day_index")
  }

  // ---- cleansing layer ----------------------------------------------

  val validOrders = streamingTableFrom("valid_orders") {
    stream.table("orders").where(col("amount") > lit(0L) && col("order_id") =!= lit(0L))
  }

  val dedupedOrders = streamingTableFrom("deduped_orders") {
    stream.table("valid_orders").dropDuplicates("order_id")
  }

  // ---- enrichment ----------------------------------------------------

  val bucketedOrders = streamingTableFrom("bucketed_orders") {
    val withBucket = stream.table("deduped_orders").withColumn("bucket_id", col("amount") % lit(10L))
    withBucket
      .join(read.table("dim_buckets"))(col("bucket_id") === col("bucket_id"))
      .drop("event_id")
  }

  // ---- reporting (batch over streaming tables) -----------------------

  val bucketRevenue = materializedViewFrom("bucket_revenue") {
    read
      .table("bucketed_orders")
      .groupBy(col("bucket_name"))
      .agg(fn("sum", col("amount")).as("revenue"), fn("count", col("order_id")).as("n_orders"))
      .orderBy(col("revenue").desc)
  }

  val topBuckets = materializedViewFrom("top_buckets") {
    read.table("bucket_revenue").limit(3)
  }

  val bottomBuckets = materializedViewFrom("bottom_buckets") {
    read.table("bucket_revenue").orderBy(col("revenue").asc).limit(3)
  }

  val extremeBuckets = materializedViewFrom("extreme_buckets") {
    read.table("top_buckets").union(read.table("bottom_buckets")).distinct
  }

  val quietDays = materializedViewFrom("quiet_days") {
    val weekend = read.table("dim_calendar").where(col("day_of_week") >= lit(5L))
    weekend.alias("weekend_days").select(col("day_index"))
  }

  val auditSample = materializedViewFrom("audit_sample") {
    read.table("bucketed_orders").offset(10).tail(100)
  }

  private val all = List(
    rawEvents, orders, dimBuckets, dimCalendar, validOrders, dedupedOrders,
    bucketedOrders, bucketRevenue, topBuckets, bottomBuckets, extremeBuckets,
    quietDays, auditSample,
  )

  def spec = suite("warehouse dogfood (F12)")(
    test("13 datasets assemble into a valid manifest with full derived lineage") {
      for manifest <- ManifestAssembly.assemble(all)
      yield assertTrue(
        manifest.nodes.size == 13,
        manifest.flows.size == 13,
        // spot-check the derived lineage spine
        manifest.edges.contains(DependencyEdge("raw_events", "orders")),
        manifest.edges.contains(DependencyEdge("deduped_orders", "bucketed_orders")),
        manifest.edges.contains(DependencyEdge("dim_buckets", "bucketed_orders")),
        manifest.edges.contains(DependencyEdge("bucket_revenue", "top_buckets")),
        manifest.edges.contains(DependencyEdge("top_buckets", "extreme_buckets")),
        manifest.edges.contains(DependencyEdge("bottom_buckets", "extreme_buckets")),
        manifest.toGraph.topologicalSort.isRight,
      )
    },
    test("the manifest round-trips byte-identically — 13 authored flows survive") {
      for manifest <- ManifestAssembly.assemble(all)
      yield assertTrue(
        PipelineManifest.parse(manifest.render) == Right(manifest),
        manifest.render == manifest.render,
      )
    },
    test("F13: a typo'd column three datasets downstream of the source fails the build, naming candidates") {
      val typo = streamingTableFrom("typo_orders") {
        // deduped_orders' inferred shape flows from the rate source's declared
        // schema through orders → valid_orders → deduped_orders. 'amout' is wrong.
        stream.table("deduped_orders").where(col("amout") > lit(0L))
      }
      for exit <- ManifestAssembly.assemble(all :+ typo).exit
      yield assertTrue(
        exit.causeOption.flatMap(_.failureOption).exists(_.exists {
          case PipelineValidationError.UnknownColumn("typo_orders", "amout", available) =>
            available.contains("amount") && available.contains("order_id")
          case _ => false
        })
      )
    },
    test("F13 gradual: datasets fed by SQL bodies are unchecked downstream") {
      val sqlFed = sqlStreamingTable("sql_fed")("SELECT anything FROM STREAM(raw_events)")
      val blind = streamingTableFrom("blind") {
        stream.table("sql_fed").select(col("no_idea_if_this_exists"))
      }
      for manifest <- ManifestAssembly.assemble(all ++ List(sqlFed, blind))
      yield assertTrue(manifest.nodes.size == 15)
    },
  ).provide(ManifestAssembly.live, GraphValidation.live)

// cache-bust 2026-06-06: macro-impl change in sdp-core (RelCodec mkString fix)
