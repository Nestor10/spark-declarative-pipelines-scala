package pipelines

import dev.sdp.core.GraphFragment
import dev.sdp.dsl.*

object Silver:
  /** Streaming join of the bronze stream against the customer dimension —
    * written in the fluent flow language, extracted at compile time. The
    * lineage edges in the manifest come from what this body actually reads.
    */
  val orders: GraphFragment = streamingTable("silver_orders") {
    val enriched = spark.readStream
      .table("bronze_orders")
      .join(spark.read.table("dim_customers"))(col("customer_id") === col("id"))
    enriched.select(col("order_id"), col("amount"), col("name").as("customer_name"))
  }
