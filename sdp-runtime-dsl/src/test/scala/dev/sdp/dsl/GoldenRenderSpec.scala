package dev.sdp.dsl

import scala.io.Source

import dev.sdp.core.GraphFragment
import dev.sdp.core.algebra.{Rel, RelCodec}
import zio.test.*

/** The frozen-oracle spec (D10 / M3). The DSL is THE frontend now; there is no
  * macro frontend left to compare against. Instead, every runtime fixture's
  * `RelCodec.render` is asserted against the FROZEN block in
  * `src/test/resources/golden-renders.txt` — the renders captured from the
  * (now-deleted) macro frontend during the cutover and proven equivalent.
  *
  * `RelCodec.render` is the oracle: a pure function of tree structure and the
  * exact form the manifest embeds (render → parseTrusted). A drift in the
  * algebra or codec changes a render and fails the matching named test.
  *
  * Coverage discipline:
  *   - one named test per construct (failure names the construct);
  *   - a test asserting no golden block is left UNMATCHED, so a fixture can
  *     never silently drop out of coverage;
  *   - the externalTable + SQL-entry-point cases have no golden block (they
  *     carry no flow / a trivial `Rel.Sql`) — kept as plain structural tests.
  *
  * Regenerating the golden file is a deliberate, reviewed act — see [[GoldenGen]].
  */
object GoldenRenderSpec extends ZIOSpecDefault:

  /** Parse the frozen golden file into `name/flowName -> render`. A block is a
    * `>>> key` header followed by its render line(s), blocks blank-separated. */
  private val golden: Map[String, String] =
    val text = {
      val in = getClass.getResourceAsStream("/golden-renders.txt")
      require(in != null, "golden-renders.txt missing from test resources")
      val src = Source.fromInputStream(in, "UTF-8")
      try src.mkString
      finally src.close()
    }
    // Split into blocks at each line that starts a new ">>> " header.
    val blocks = collection.mutable.ListBuffer.empty[List[String]]
    var current = collection.mutable.ListBuffer.empty[String]
    for line <- text.linesIterator do
      if line.startsWith(">>> ") then
        if current.nonEmpty then blocks += current.toList
        current = collection.mutable.ListBuffer(line)
      else if current.nonEmpty then current += line
    if current.nonEmpty then blocks += current.toList
    blocks.iterator.flatMap { lines =>
      val key     = lines.head.stripPrefix(">>> ")
      // drop trailing blank lines that the blank-line separator leaves behind
      val render  = lines.tail.reverse.dropWhile(_.isEmpty).reverse.mkString("\n")
      if key.nonEmpty then Some(key -> render) else None
    }.toMap

  /** One named test: the fixture's single flow renders to its frozen block. */
  private def goldenTest(name: String, frag: GraphFragment) =
    test(s"$name: runtime render matches the frozen golden block") {
      frag.flows match
        case List(flow) =>
          val key = s"$name/${flow.name}"
          golden.get(key) match
            case Some(expected) =>
              assertTrue(RelCodec.render(flow.relation) == expected)
            case None =>
              assertTrue(false) // missing golden block for this fixture
        case _ =>
          assertTrue(false) // every corpus fixture must carry exactly one flow
    }

  // ------------------------------------------------------------------
  // structural cases that have NO golden block (no flow / trivial Rel.Sql)
  // ------------------------------------------------------------------
  private val extTableTests = test("externalTable: single external node, no flow") {
    val frag = RuntimeFixtures.bronzeOrders
    assertTrue(
      frag.flows.isEmpty,
      frag.nodes.map(_.id) == List("bronze.orders"),
      RuntimeFixtures.bronzeCustomerOrders.nodes.map(_.id) == List("bronze.customer_orders"),
    )
  }

  private val sqlEntryPointTests = test("SQL entry points have the expected node/flow shape") {
    val rtSqlST = sqlStreamingTable("sql_st")("SELECT 1")
    val rtMv    = materializedView("mv_sql")("SELECT 2")
    val rtTv    = temporaryView("tv_sql")("SELECT 3")
    assertTrue(
      rtSqlST.flows.map(_.relation) == List(Rel.Sql("SELECT 1")),
      rtMv.flows.isEmpty,
      rtMv.nodes.map(_.id) == List("mv_sql"),
      rtTv.nodes.map(_.id) == List("tv_sql"),
    )
  }

  // ------------------------------------------------------------------
  // coverage guard — every golden block is claimed by some construct test
  // ------------------------------------------------------------------
  private val noOrphans = test("no golden block is left unmatched") {
    // Order-independent: recompute the full covered key set straight from the
    // corpus, then assert every frozen golden key is claimed by some fixture.
    val coveredKeys = GoldenCorpus.all.flatMap { (name, frag) =>
      frag.flows.map(f => s"$name/${f.name}")
    }.toSet
    val orphans = golden.keySet -- coveredKeys
    assertTrue(orphans.isEmpty, golden.nonEmpty)
  }

  def spec = suite("GoldenRenderSpec — runtime DSL renders match the frozen oracle")(
    extTableTests,
    sqlEntryPointTests,
    noOrphans,
    suite("warehouse fixtures")(GoldenCorpus.warehouse.map((n, f) => goldenTest(n, f))*),
    suite("construct corpus")(GoldenCorpus.corpus.map((n, f) => goldenTest(n, f))*),
  )
