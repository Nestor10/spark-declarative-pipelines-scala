package pipelines

import dev.sdp.connect.app.SdpApp

/** Drop-in replacement copied over `Warehouse.scala` by the scripted `test`
  * once `Gold.scala` is added: the pipeline now also lists `Gold.orders`, so
  * the next `sdpManifest` must include `gold_orders`. Adding the source +
  * editing the pipeline changes the compiled products, invalidating the
  * classpath-keyed cache and forcing a recompute.
  */
object Warehouse extends SdpApp:
  def pipeline: Pipeline = List(
    Bronze.orders,
    Bronze.customers,
    Gold.orders,
  )
