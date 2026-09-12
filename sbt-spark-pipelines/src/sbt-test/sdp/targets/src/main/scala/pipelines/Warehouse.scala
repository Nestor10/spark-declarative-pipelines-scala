package pipelines

import dev.sdp.core.GraphFragment
import dev.sdp.connect.app.SdpApp
import dev.sdp.dsl.*

/** The pipeline the `targets` suite promotes between environments.
  *
  * Note what is NOT here: any mention of an environment. Dataset names are
  * unqualified, so where they land is decided by the target's
  * catalog/database at registration — principle P1, "parameters vary WHERE,
  * never WHAT". That is precisely why the manifest bytes can be identical for
  * dev and prod.
  */
object Warehouse extends SdpApp:

  val orders: GraphFragment = table("bronze_orders")

  val enriched: GraphFragment = streamingTable("silver_orders") {
    spark.readStream.table("bronze_orders")
  }

  def pipeline: Pipeline = List(orders, enriched)
