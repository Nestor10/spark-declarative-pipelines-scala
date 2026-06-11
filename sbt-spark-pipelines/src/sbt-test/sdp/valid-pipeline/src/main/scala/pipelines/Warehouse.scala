package pipelines

import dev.sdp.connect.app.SdpApp

/** The pipeline object the plugin evaluates (`sdpPipelineClass`). In the D10
  * runtime-builder world the author lists every fragment explicitly; mixing in
  * `SdpApp` also makes the uber jar a standalone runner (`java -jar app.jar run`).
  */
object Warehouse extends SdpApp:
  def pipeline: Pipeline = List(
    Bronze.orders,
    Bronze.customers,
    Rates.rates,
    Silver.orders,
  )
