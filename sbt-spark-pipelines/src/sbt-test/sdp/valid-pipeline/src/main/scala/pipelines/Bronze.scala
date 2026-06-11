package pipelines

import dev.sdp.core.GraphFragment
import dev.sdp.dsl.*

object Bronze:
  val orders: GraphFragment    = table("bronze_orders")
  val customers: GraphFragment = table("dim_customers")
