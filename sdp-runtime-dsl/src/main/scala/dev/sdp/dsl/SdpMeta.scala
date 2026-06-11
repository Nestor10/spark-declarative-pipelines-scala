package dev.sdp.dsl

import dev.sdp.core.GraphFragment

/** The TASTy discovery marker.
  *
  * Every DSL macro expands to exactly one call of [[SdpMeta.embed]] whose
  * argument is a *string literal* in the canonical fragment line format.
  *
  * LOAD-BEARING DETAIL (verified by the F7a spike): the DSL entry points are
  * `transparent inline`. Plain `inline` expansions happen in the `inlining`
  * phase, *after* pickling — the call site's `.tasty` would contain only the
  * unexpanded DSL call and the scanner would find nothing. `transparent`
  * forces expansion during typing, *before* pickling, so this literal is
  * serialized into the caller's `.tasty` — where the sbt plugin's
  * tasty-query scanner finds it without classloading or executing user code.
  * Removing `transparent` from the DSL breaks fragment discovery; the
  * scanner spec (`TastyFragmentScannerSpec`) guards this.
  *
  * Zinc owns `.tasty` lifecycle: delete or rename the source file and the
  * constant disappears with it. No sidecar staleness, ever.
  *
  * Do not call this directly; it is the macros' output, not an author API.
  */
object SdpMeta:

  /** Fully-qualified name the scanner matches on. Kept in one place so the
    * macro emitter and the plugin scanner cannot drift apart.
    */
  final val MarkerOwner  = "dev.sdp.dsl.SdpMeta"
  final val MarkerMethod = "embed"

  /** Runtime semantics of an embedded fragment: parse the trusted constant.
    * A malformed constant is a macro bug and dies as a defect.
    */
  def embed(canonicalLines: String): GraphFragment =
    GraphFragment.parseTrusted(canonicalLines)
