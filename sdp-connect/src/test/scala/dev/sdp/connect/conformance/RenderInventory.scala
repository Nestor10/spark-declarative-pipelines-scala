package dev.sdp.connect.conformance

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}

/** Regenerates the golden inventory snapshot. Run after a *conscious* Spark
  * artifact upgrade, then review the git diff of the snapshot — that diff IS
  * the upstream change report:
  *
  * {{{
  * sbt 'sdpConnect/Test/runMain dev.sdp.connect.conformance.RenderInventory \
  *   sdp-connect/src/test/resources/spark-connect-inventory.txt'
  * }}}
  */
object RenderInventory:
  def main(args: Array[String]): Unit =
    val out = Paths.get(args.headOption.getOrElse(sys.error("usage: RenderInventory <output-path>")))
    Files.createDirectories(out.getParent)
    Files.write(out, ConnectInventory.render.getBytes(UTF_8))
    println(s"wrote ${out.toAbsolutePath}")
