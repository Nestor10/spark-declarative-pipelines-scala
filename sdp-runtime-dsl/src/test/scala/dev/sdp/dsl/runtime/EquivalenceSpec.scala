package dev.sdp.dsl.runtime

import dev.sdp.core.GraphFragment
import dev.sdp.core.algebra.{Rel, RelCodec}
import zio.test.*

/** DoD #1: for every Warehouse fragment, the runtime-built `Rel` renders
  * IDENTICALLY (via `RelCodec.render`) to the macro frontend's `Rel` for the
  * same body — body-for-body, only the imports differing.
  *
  * `RelCodec.render` is the equivalence oracle: it's a pure function of tree
  * structure and is the exact form the macro embeds (render → parseTrusted →
  * the Rel that survives in the flow). Comparing renders therefore compares
  * the algebra the two frontends produce, ignoring incidental List/Set
  * ordering in the surrounding `GraphFragment`.
  */
object EquivalenceSpec extends ZIOSpecDefault:

  /** Pull the single flow's relation out of a fragment (every Warehouse
    * fragment here is a single-flow table/view; externalTable has none). */
  private def relOf(frag: GraphFragment): Option[Rel] =
    frag.flows.headOption.map(_.relation)

  private def sameRel(name: String, runtime: GraphFragment, macroF: GraphFragment) =
    test(s"$name: runtime Rel renders identically to macro Rel") {
      (relOf(runtime), relOf(macroF)) match
        case (Some(r), Some(m)) =>
          assertTrue(RelCodec.render(r) == RelCodec.render(m))
        case _ =>
          assertTrue(false) // both fragments must carry a flow
    }

  // ------------------------------------------------------------------
  // node-shape equivalence for the external tables (no flow / no Rel)
  // ------------------------------------------------------------------
  private val extTableTests = test("externalTable: node shape matches macro frontend") {
    assertTrue(
      RuntimeFixtures.bronzeOrders.nodes == MacroFixtures.bronzeOrders.nodes,
      RuntimeFixtures.bronzeCustomerOrders.nodes == MacroFixtures.bronzeCustomerOrders.nodes,
      RuntimeFixtures.bronzeOrders.flows.isEmpty,
      MacroFixtures.bronzeOrders.flows.isEmpty,
    )
  }

  def spec = suite("EquivalenceSpec — runtime builder ≡ macro frontend (RelCodec render)")(
    extTableTests,
    sameRel("rates",                RuntimeFixtures.rates,             MacroFixtures.rates),
    sameRel("daily_orders_by_state", RuntimeFixtures.dailyOrdersByState, MacroFixtures.dailyOrdersByState),
    sameRel("orders_enriched",      RuntimeFixtures.ordersEnriched,    MacroFixtures.ordersEnriched),
    sameRel("regions",              RuntimeFixtures.regions,           MacroFixtures.regions),
  )
