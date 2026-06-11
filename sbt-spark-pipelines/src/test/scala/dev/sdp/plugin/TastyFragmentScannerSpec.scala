package dev.sdp.plugin

import java.io.File
import java.nio.file.{Path, Paths}

import dev.sdp.core.*
import dev.sdp.dsl.SdpMeta
import dev.sdp.plugin.fixture.SpikeFixture
import zio.test.*

/** The F7a spike, as a permanent regression test. Verifies the two
  * assumptions the fragment-discovery architecture rests on:
  *
  *   1. macro expansions (including the `SdpMeta.embed` literal) are pickled
  *      into the call site's `.tasty` on this Scala version;
  *   2. tasty-query walks `Inlined` nodes and surfaces the literal.
  *
  * If this spec breaks on a compiler upgrade, the fallback is Model A
  * (reflective loading with worker isolation) — see ROADMAP.
  */
object TastyFragmentScannerSpec extends ZIOSpecDefault:

  /** This module's test-classes directory — where SpikeFixture's .tasty lives. */
  private val testClassesDir: Path =
    Paths.get(SpikeFixture.getClass.getProtectionDomain.getCodeSource.getLocation.toURI)

  /** The test JVM's full classpath, for symbol resolution.
    *
    * Sourced from the classloader chain *and* `java.class.path`: depending on
    * how the test is launched (runMain fork vs test-framework runner) either
    * source alone can be incomplete (e.g. a pathing jar standing in for the
    * real entries). A pathing jar's `Class-Path` manifest is expanded
    * explicitly.
    */
  private val dependencyClasspath: List[Path] =
    def loaderUrls(cl: ClassLoader): List[Path] = cl match
      case null => Nil
      case url: java.net.URLClassLoader =>
        url.getURLs.toList.map(u => Paths.get(u.toURI)) ++ loaderUrls(cl.getParent)
      case other => loaderUrls(other.getParent)

    def manifestClasspath(jar: Path): List[Path] =
      try
        val mf = java.util.jar.JarFile(jar.toFile).getManifest
        Option(mf)
          .flatMap(m => Option(m.getMainAttributes.getValue("Class-Path")))
          .fold(List.empty[Path]) {
            _.split("\\s+").toList.filter(_.nonEmpty).map(entry => Paths.get(java.net.URI.create(entry)))
          }
      catch case _: Exception => Nil

    val fromProp =
      System.getProperty("java.class.path").split(File.pathSeparator).toList.filter(_.nonEmpty).map(Paths.get(_))
    val direct   = (loaderUrls(getClass.getClassLoader) ++ fromProp).distinct
    val expanded = direct ++ direct.filter(_.toString.endsWith(".jar")).flatMap(manifestClasspath)
    expanded.distinct.filter(p => java.nio.file.Files.exists(p))

  def spec = suite("TastyFragmentScanner (F7a spike)")(
    test("recovers every macro-embedded fragment from .tasty — values, not just structure") {
      val fragments = TastyFragmentScanner.scan(List(testClassesDir), dependencyClasspath)
      assertTrue(
        fragments.contains(
          GraphFragment(List(PipelineNode.Table("spike_bronze", "delta")), Set.empty)
        ),
        fragments.contains(
          GraphFragment(
            List(PipelineNode.StreamingTable("spike_silver", "delta")),
            Set(DependencyEdge("spike_bronze", "spike_silver")),
          )
        ),
      )
    },
    test("discovery is declaration-based: a fragment inside a never-called def is found") {
      val fragments = TastyFragmentScanner.scan(List(testClassesDir), dependencyClasspath)
      assertTrue(
        fragments.contains(
          GraphFragment(List(PipelineNode.Table("spike_unreferenced", "delta")), Set.empty)
        )
      )
    },
    test("no double-counting when targets also appear in the dependency classpath") {
      val fragments = TastyFragmentScanner.scan(List(testClassesDir), dependencyClasspath)
      val bronzeCount = fragments.count(
        _ == GraphFragment(List(PipelineNode.Table("spike_bronze", "delta")), Set.empty)
      )
      assertTrue(bronzeCount == 1)
    },
    test("scanner marker names are pinned to the real SdpMeta definitions") {
      assertTrue(
        TastyFragmentScanner.SdpMarker.Owner == SdpMeta.MarkerOwner,
        TastyFragmentScanner.SdpMarker.Method == SdpMeta.MarkerMethod,
      )
    },
    test("recovered fragments assemble into a valid manifest end-to-end") {
      val fragments = TastyFragmentScanner.scan(List(testClassesDir), dependencyClasspath)
      val relevant = fragments.filter(_.nodes.exists(n => n.id.startsWith("spike_")))
      // Drop the unreferenced orphan; bronze+silver must form a valid graph.
      val graphFragments = relevant.filterNot(_.nodes.exists(_.id == "spike_unreferenced"))
      val merged         = GraphFragment.mergeAll(graphFragments)
      assertTrue(PipelineGraph.fromFragments(merged.nodes, merged.edges).isRight)
    },
  )
