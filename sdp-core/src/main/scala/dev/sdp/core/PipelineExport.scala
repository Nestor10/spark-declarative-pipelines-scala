package dev.sdp.core

/** The cross-classloader export boundary (D10).
  *
  * The sbt plugin evaluates a user's pipeline object in a *child* classloader
  * built over the project's runtime classpath, with NO parent delegation for
  * user/sdp classes. That means a `GraphFragment` produced in the child loader
  * is a DIFFERENT `Class` from the plugin's own `GraphFragment` — you cannot
  * hand the objects across the boundary directly.
  *
  * So the boundary is the fragment STRING — exactly the contract the old TASTy
  * embedding used: each fragment is rendered with [[GraphFragment.render]] (the
  * shared line dialect) into a plain `java.lang.String`. Strings ARE shared
  * across loaders (loaded by the bootstrap loader), so an `Array[String]` is
  * trivially returnable. The plugin decodes each string with its own
  * [[GraphFragment.parse]] and feeds the existing assembly/validation flow
  * unchanged.
  *
  * The signature is deliberately `Any -> Array[String]` (only `java.lang` /
  * array types) so the plugin can invoke it reflectively in the child loader
  * without sharing any Scala type:
  *
  * {{{
  * val m = childCls.getMethod("encodeAll", classOf[Object])
  * val lines = m.invoke(exportModule, pipelineValue).asInstanceOf[Array[String]]
  * }}}
  *
  * Round-trips through [[GraphFragment.parse]]: `parse(render(f)) == Right(f)`
  * for any fragment (guarded by `PipelineExportSpec`).
  */
object PipelineExport:

  /** Render every fragment of `pipeline` (a `List`/`Seq` of `GraphFragment`, or
    * a single `GraphFragment`) to its canonical line-dialect string.
    *
    * Accepts `Any` so it is reflection-callable across classloaders. A value
    * that is neither a fragment nor a collection of fragments is a programming
    * error in the caller (wrong `sdpPipelineClass` member type) and fails loud.
    */
  def encodeAll(pipeline: Any): Array[String] =
    val fragments: Iterable[GraphFragment] = pipeline match
      case f: GraphFragment        => List(f)
      case it: Iterable[?]         => it.map(asFragment).toList
      case arr: Array[?]           => arr.iterator.map(asFragment).toList
      case other =>
        throw new IllegalArgumentException(
          s"sdp: pipeline value must be a List[GraphFragment] (or a single GraphFragment), " +
            s"got ${describe(other)}"
        )
    fragments.iterator.map(GraphFragment.render).toArray

  private def asFragment(x: Any): GraphFragment = x match
    case f: GraphFragment => f
    case other =>
      throw new IllegalArgumentException(
        s"sdp: pipeline collection element is not a GraphFragment — got ${describe(other)}"
      )

  private def describe(x: Any): String =
    if x == null then "null" else x.getClass.getName
