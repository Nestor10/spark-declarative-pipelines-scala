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

    test("createAutoCdcFlow into createStreamingTable assembles into a valid v3 manifest") {
      // bronze CDC source → AUTO CDC into a streaming-table target
      val source = externalTable("bronze.cdc")
      val dim    = createStreamingTable("dim_customers")
      val cdc = createAutoCdcFlow(
        target = "dim_customers",
        source = "bronze.cdc",
        keys = Seq(col("id")),
        sequenceBy = col("seq"),
        applyAsDeletes = Some(col("op") === lit("DELETE")),
      )
      for manifest <- ManifestAssembly.assemble(Pipeline(source, dim, cdc))
      yield assertTrue(
        manifest.formatVersion == 3,
        manifest.flows.map(_.name) == List("dim_customers_auto_cdc"),
        manifest.toGraph.topologicalSort.isRight,
      )
    },

    test("createAutoCdcFlow with a String-keyed overload works the same") {
      val source = externalTable("bronze.cdc")
      val dim    = createStreamingTable("dim_customers")
      val cdc    = createAutoCdcFlow("dim_customers", "bronze.cdc", Seq("id"), "seq")
      for manifest <- ManifestAssembly.assemble(Pipeline(source, dim, cdc))
      yield assertTrue(manifest.flows.size == 1, manifest.toGraph.topologicalSort.isRight)
    },

    test("storedAsScdType = 2 + track-history columns author and validate today (S2)") {
      val source = externalTable("bronze.cdc")
      val dim    = createStreamingTable("dim_customers")
      val cdc = createAutoCdcFlow(
        target = "dim_customers",
        source = "bronze.cdc",
        keys = Seq(col("id")),
        sequenceBy = col("seq"),
        storedAsScdType = 2,
        trackHistoryExceptColumnList = Seq(col("audit_ts")),
      )
      for manifest <- ManifestAssembly.assemble(Pipeline(source, dim, cdc))
      yield assertTrue(
        manifest.formatVersion == 3,
        manifest.flows.map(_.name) == List("dim_customers_auto_cdc"),
        manifest.flows.head.details match
          case d: dev.sdp.core.FlowDetails.AutoCdc =>
            d.scdType == dev.sdp.core.ScdType.Scd2 &&
            d.trackHistoryExceptColumnList.sizeIs == 1
          case _ => false,
      )
    },

    test("an SCD type outside {1, 2} is refused at the call site") {
      // Not representable downstream (`ScdType` has exactly the proto's cases),
      // so the Int is checked where the Int still exists.
      val thrown =
        try { val _ = createAutoCdcFlow("dim", "src", Seq(col("id")), col("seq"), storedAsScdType = 3); None }
        catch { case e: IllegalArgumentException => Some(e.getMessage) }
      assertTrue(thrown.exists(m => m.contains("1 or 2") && m.contains("3")))
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
