package dev.sdp.dsl.runtime

import dev.sdp.core.GraphFragment
import dev.sdp.dsl.*
import dev.sdp.dsl.functions.*

/** The SAME Warehouse bodies as [[RuntimeFixtures]], built with the MACRO
  * frontend (`dev.sdp.dsl.*`). Obtained exactly as WarehouseDogfoodSpec does:
  * the `streamingTable`/`materializedView`/`externalTable` inline entry points
  * expand at compile time into embedded `GraphFragment`s.
  *
  * The bodies are line-for-line identical to RuntimeFixtures, with the
  * `cols[S]` typed-column substitution applied on BOTH sides (plain `col(...)`)
  * so the comparison is body-for-body, import-only difference.
  */
object MacroFixtures:

  val bronzeOrders: GraphFragment         = externalTable("bronze.orders")
  val bronzeCustomerOrders: GraphFragment = externalTable("bronze.customer_orders")

  val rates: GraphFragment = streamingTable("rates") {
    spark.readStream
      .format("rate")
      .schema("timestamp TIMESTAMP, value BIGINT")
      .load()
      .withColumn("test", upper(col("timestamp")))
      .select(col("value"), col("timestamp").as("seen_at"))
  }

  val dailyOrdersByState: GraphFragment = materializedView("daily_orders_by_state") {
    spark
      .table("bronze.customer_orders")
      .groupBy("state", "order_date")
      .count()
      .withColumnRenamed("count", "order_count")
      .withColumn("test", col("order_count"))
  }

  val ordersEnriched: GraphFragment = materializedView("orders_enriched") {
    spark
      .table("bronze.orders")
      .where(col("amount") > lit(0L))
      .withColumn("rn", row_number().over(Window.partitionBy(col("state")).orderBy(col("amount").desc)))
      .groupBy("state")
      .agg(sum(col("amount")).as("total"), count(star).as("n"))
  }

  val regions: GraphFragment = materializedView("regions") {
    spark.createDataFrame(Seq(("CA", "West"), ("NY", "East"), ("TX", "South"))).toDF("state", "region")
  }

  val all: List[GraphFragment] = List(
    bronzeOrders, bronzeCustomerOrders, rates, dailyOrdersByState, ordersEnriched, regions,
  )
