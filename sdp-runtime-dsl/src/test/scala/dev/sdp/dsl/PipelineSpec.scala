package dev.sdp.dsl

import dev.sdp.app.{GraphValidation, ManifestAssembly}
import dev.sdp.core.PipelineValidationError
import zio.test.*

/** DoD #2: the explicit `Pipeline(...)` value drives the EXISTING
  * `ManifestAssembly` + `GraphValidation` unchanged.
  *
  *   - the six Warehouse fragments assemble into a valid manifest;
  *   - omitting `bronze.customer_orders` leaves `daily_orders_by_state`'s read
  *     dangling → the existing dangling-dependency error;
  *   - a 2-node cycle built with the runtime builder is detected.
  */
object PipelineSpec extends ZIOSpecDefault:

  def spec = suite("PipelineSpec — runtime Pipeline drives existing assembly/validation")(

    test("the six Warehouse fragments assemble into a valid manifest") {
      for manifest <- ManifestAssembly.assemble(Pipeline(RuntimeFixtures.all*))
      yield assertTrue(
        // 4 owned datasets (rates, daily_orders_by_state, orders_enriched,
        // regions) + 2 external tables = 6 nodes
        manifest.nodes.size == 6,
        manifest.flows.size == 4,
        manifest.toGraph.topologicalSort.isRight,
      )
    },

    test("omitting bronze.customer_orders makes daily_orders_by_state's read dangle") {
      // drop the external table that daily_orders_by_state reads
      val missing = Pipeline(
        RuntimeFixtures.bronzeOrders,
        // bronzeCustomerOrders intentionally omitted
        RuntimeFixtures.rates,
        RuntimeFixtures.dailyOrdersByState,
        RuntimeFixtures.ordersEnriched,
        RuntimeFixtures.regions,
      )
      for exit <- ManifestAssembly.assemble(missing).exit
      yield assertTrue(
        exit.causeOption.flatMap(_.failureOption).exists(_.exists {
          case PipelineValidationError.DanglingEdges(ids) =>
            ids.contains("bronze.customer_orders")
          case _ => false
        })
      )
    },

    test("a 2-node cycle built via the runtime builder is detected") {
      val a = materializedView("cyc_a") { spark.table("cyc_b") }
      val b = materializedView("cyc_b") { spark.table("cyc_a") }
      for exit <- ManifestAssembly.assemble(Pipeline(a, b)).exit
      yield assertTrue(
        exit.causeOption.flatMap(_.failureOption).exists(_.exists {
          case PipelineValidationError.CycleDetected(path) =>
            path.contains("cyc_a") && path.contains("cyc_b")
          case _ => false
        })
      )
    },
  ).provide(ManifestAssembly.live, GraphValidation.live)
