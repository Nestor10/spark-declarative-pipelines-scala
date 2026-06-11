package pipelines

import dev.sdp.connect.app.SdpApp

/** Drop-in replacement used by the scripted `test` after Silver.scala is
  * deleted: the pipeline no longer lists `Silver.orders`, so the next
  * `sdpManifest` recomputes WITHOUT silver_orders — no `clean` needed. This is
  * the classload-eval analogue of the old "delete a source, its fragment
  * vanishes" property: editing the pipeline reshapes the manifest incrementally.
  */
object Warehouse extends SdpApp:
  def pipeline: Pipeline = List(
    Bronze.orders,
    Bronze.customers,
    Rates.rates,
  )
