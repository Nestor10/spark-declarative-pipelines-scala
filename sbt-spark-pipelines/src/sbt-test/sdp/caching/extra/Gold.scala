package pipelines

import dev.sdp.core.GraphFragment
import dev.sdp.dsl.*

object Gold:
  val orders: GraphFragment = streamingTable("gold_orders") {
    spark.readStream.table("bronze_orders")
  }
