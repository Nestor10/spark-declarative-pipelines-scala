package pipelines

import dev.sdp.connect.app.SdpApp

/** The pipeline object the plugin evaluates. Lists the two mutually-reading
  * streaming tables; assembling them must fail with the cycle path before
  * anything reaches a server.
  */
object Warehouse extends SdpApp:
  def pipeline: Pipeline = List(Cycle.a, Cycle.b)
