package dev.sdp.dsl

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}

import dev.sdp.core.GraphFragment
import dev.sdp.core.algebra.RelCodec

/** Generator for `src/test/resources/golden-renders.txt` — the FROZEN oracle
  * `GoldenRenderSpec` checks every runtime fixture against.
  *
  * ⚠️ DANGER — REGENERATING THE GOLDEN FILE REDEFINES THE ORACLE. ⚠️
  *
  * The golden file is the single source of truth for what the DSL is *supposed*
  * to render. `GoldenRenderSpec` passes iff each fixture still renders its
  * frozen block. Running this main OVERWRITES that truth with whatever the
  * current code happens to produce — so a render regression would be silently
  * baptised as the new correct answer.
  *
  * Regeneration is legitimate ONLY after a REVIEWED, INTENTIONAL change to the
  * algebra (`dev.sdp.core.algebra.*`) or `RelCodec`, where the new renders have
  * been eyeballed and the diff is part of an approved commit. Never run it to
  * "make the test green". When in doubt, fix the code, not the oracle.
  *
  * Originally the golden file was frozen from the (now-deleted) macro frontend
  * during the D10 cutover; the runtime builder was proven render-identical to
  * it. Renders are stable by construction (render → parse is the canonical
  * round-trip the manifest itself uses), so the same fixtures emit
  * byte-identical blocks on every run.
  *
  * {{{ sbt 'sdp/Test/runMain dev.sdp.dsl.GoldenGen' }}}
  *
  * Format: one block per construct — `>>> <name>/<flowName>` line, the render
  * on the following line(s), blank line between blocks.
  */
object GoldenGen:

  private def blocks(name: String, frag: GraphFragment): List[String] =
    frag.flows.map(f => s">>> $name/${f.name}\n${RelCodec.render(f.relation)}")

  def main(args: Array[String]): Unit =
    val out = GoldenCorpus.all.flatMap((name, frag) => blocks(name, frag)).mkString("", "\n\n", "\n")
    // CWD depends on how sbt was invoked (module dir under some invocations,
    // the BUILD ROOT under the thin client) — resolve the module explicitly so
    // a regeneration can never leave a stray `src/` at the repo root.
    val relative = Paths.get("src/test/resources/golden-renders.txt")
    val path =
      if Files.isDirectory(Paths.get("sdp").resolve(relative.getParent)) then
        Paths.get("sdp").resolve(relative)
      else relative
    Files.createDirectories(path.getParent)
    val _ = Files.write(path, out.getBytes(UTF_8))
    println(s"wrote ${path.toAbsolutePath} (${out.linesIterator.count(_.startsWith(">>>"))} flows)")
