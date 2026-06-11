package dev.sdp.connect.app

import dev.sdp.core.PipelineValidationError

/** The single place that turns expected validation failures into the
  * human-readable block a build (or the `SdpApp` runner) prints.
  *
  * Lifted out of the sbt plugin so the plugin and the library-first runner
  * render identical messages — `PipelineValidationError`s are EXPECTED errors
  * (Zionomicon ch. 3), and an author should see the same words whether the
  * graph was rejected at `sbt sdpManifest` or at `SdpApp validate`.
  */
object ValidationRendering:

  /** One error per line, each prefixed `  - `, in the order given.
    * The caller supplies the heading. */
  def renderErrors(errors: List[PipelineValidationError]): String =
    errors.map(e => s"  - ${e.describe}").mkString("\n")

  /** The full block the plugin's `sdpManifest` task emits on an invalid
    * graph, reused verbatim so messages don't drift between surfaces. */
  def invalidGraphMessage(errors: List[PipelineValidationError]): String =
    s"SDP pipeline graph is invalid:\n${renderErrors(errors)}"
