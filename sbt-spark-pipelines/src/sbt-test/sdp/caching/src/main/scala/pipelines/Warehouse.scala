package pipelines

import dev.sdp.connect.app.SdpApp

/** The pipeline object the plugin evaluates (`sdpPipelineClass`). Lists only the
  * bronze tables at first; the scripted `test` later swaps in a version that
  * also includes `Gold.orders` to exercise cache invalidation.
  */
object Warehouse extends SdpApp:
  def pipeline: Pipeline = List(
    Bronze.orders,
    Bronze.customers,
  )
