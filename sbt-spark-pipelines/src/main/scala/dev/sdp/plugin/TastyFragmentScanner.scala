package dev.sdp.plugin

import java.nio.file.Path

import dev.sdp.core.GraphFragment
import tastyquery.Contexts.Context
import tastyquery.Names.{SignedName, TermName}
import tastyquery.Symbols.TermSymbol
import tastyquery.Trees.*
import tastyquery.Traversers.TreeTraverser
import tastyquery.jdk.ClasspathLoaders

/** Discovers macro-embedded pipeline fragments by reading `.tasty` files.
  *
  * The DSL macros expand every declaration into `SdpMeta.embed("<lines>")`
  * with a string *literal* argument. Literals are pickled into the call
  * site's `.tasty`, so this scanner recovers every fragment by structural
  * tree inspection alone:
  *
  *   - no classloader over user code (no Metaspace leaks, no ServiceLoader
  *     collisions in a warm sbt server);
  *   - Zinc-correct by construction (deleting a source deletes its `.tasty`,
  *     and the fragment vanishes from the next scan — no `clean` needed).
  *
  * See ROADMAP "Fragment discovery: decided" for the full decision record.
  */
object TastyFragmentScanner:

  /** Scan `targets` (the project's own class directories) for embedded
    * fragments, resolving symbols against `targets ++ dependencies` plus the
    * JDK's `java.base` (tasty-query loads only what it is given — the Java
    * standard library must be supplied explicitly via the `jrt:` filesystem).
    *
    * Deterministic: symbols are visited in sorted fully-qualified-name order,
    * and embeds within one definition surface in source order.
    */
  def scan(targets: List[Path], dependencies: List[Path]): List[GraphFragment] =
    val targetPaths = targets.map(_.toAbsolutePath.normalize)
    // fullClasspath-style inputs typically repeat the target class dirs;
    // loading an entry twice would double every discovered fragment.
    val depPaths = dependencies
      .map(_.toAbsolutePath.normalize)
      .filterNot(targetPaths.contains)

    val classpath      = ClasspathLoaders.read(targetPaths ++ depPaths ++ List(javaBase))
    given ctx: Context = Context.initialize(classpath)

    // Classpath entries align positionally with the input paths; enumerate
    // symbols only from the target entries — dependencies are for resolution.
    val targetEntries = classpath.take(targetPaths.size)

    val collected =
      for
        entry  <- targetEntries
        symbol <- ctx.findSymbolsByClasspathEntry(entry).toList.sortBy(_.displayFullName)
        tree   <- symbol.tree.toList
      yield harvest(tree)

    collected.flatten.map(GraphFragment.parseTrusted)

  /** Collect every `SdpMeta.embed(<string literal>)` argument in the tree,
    * in source order. Wrapper nodes (`Inlined`, blocks, typed ascriptions)
    * are handled by the traverser's default descent.
    */
  private def harvest(root: Tree)(using Context): List[String] =
    val found = List.newBuilder[String]
    object traverser extends TreeTraverser:
      override def traverse(tree: Tree): Unit =
        tree match
          case Apply(fun @ Select(_, _), List(arg))
              if plainName(fun.name) == SdpMarker.Method && isMarkerOwner(fun) =>
            stringLiteral(arg).foreach(found += _)
          case _ => ()
        super.traverse(tree)
    traverser.traverse(root)
    found.result()

  /** Extract a string literal, unwrapping the `Inlined` nodes the inliner
    * leaves around macro-produced constants.
    */
  private def stringLiteral(tree: TermTree): Option[String] =
    tree match
      case Literal(constant) =>
        constant.value match
          case s: String => Some(s)
          case _         => None
      case Inlined(expr, _, _) => stringLiteral(expr)
      case _                   => None

  /** Method references in TASTy carry signatures (`SignedName`); strip down
    * to the bare term name for comparison.
    */
  private def plainName(name: TermName): String =
    name match
      case SignedName(underlying, _, _) => underlying.toString
      case other                        => other.toString

  /** Does this `Select` resolve to a method owned by the SdpMeta marker
    * object? Resolution failures are treated as non-matches rather than
    * crashes — the scanner must be total over arbitrary user code.
    */
  private def isMarkerOwner(fun: Select)(using Context): Boolean =
    try
      fun.symbol match
        case term: TermSymbol => term.owner.displayFullName == SdpMarker.OwnerModuleClass
        case _                => false
    catch case _: Exception => false

  /** Names duplicated from `dev.sdp.dsl.SdpMeta` so the plugin does not need
    * the DSL module on its compile classpath. The spike spec pins these
    * against the real definitions.
    */
  private[plugin] object SdpMarker:
    final val Owner  = "dev.sdp.dsl.SdpMeta"
    final val Method = "embed"

    /** The object's *module class* name as tasty-query reports owners. */
    final val OwnerModuleClass = Owner + "$"

  /** The JDK standard library, via the `jrt:` filesystem. */
  private lazy val javaBase: Path =
    java.nio.file.FileSystems
      .getFileSystem(java.net.URI.create("jrt:/"))
      .getPath("modules", "java.base")
