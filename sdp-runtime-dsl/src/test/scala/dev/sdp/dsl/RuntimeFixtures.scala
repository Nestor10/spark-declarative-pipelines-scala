package dev.sdp.dsl

import dev.sdp.core.GraphFragment
import dev.sdp.dsl.functions.*

/** The Warehouse example built with the runtime plan-builder DSL.
  *
  * SUBSTITUTION (noted in SPIKE_NOTES.md): `orders_enriched`'s reference body
  * uses `cols[(order_id: Long, amount: Long, state: String)]` typed columns
  * (`c.amount`). The runtime builder can't synthesize `selectDynamic` fields
  * without a macro, so we use plain `col("amount")` etc. Because `c.amount`
  * lowers to `Ex.Col("amount")` in the macro too, the rendered Rel is
  * identical — the substitution is render-invisible.
  */
object RuntimeFixtures:

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
    // reference uses cols[(order_id: Long, amount: Long, state: String)]; we
    // substitute plain col(...) — render-identical (see header note).
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
