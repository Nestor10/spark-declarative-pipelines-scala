package dev.sdp.dsl

import scala.compiletime.testing.typeCheckErrors

import dev.sdp.app.{GraphValidation, ManifestAssembly}
import dev.sdp.core.*
import zio.test.*

object DslSpec extends ZIOSpecDefault:

  def spec = suite("DSL macros")(
    suite("table (F5)")(
      test("extracts a literal table name into a single-node fragment") {
        val fragment = table("bronze_orders")
        assertTrue(
          fragment == GraphFragment(
            List(PipelineNode.Table("bronze_orders", "delta")),
            Set.empty,
          )
        )
      },
      test("rejects a non-literal table name at compile time") {
        val errors = typeCheckErrors(
          """
          import dev.sdp.dsl.*
          val dynamic: String = "runtime_value"
          val bad = table(dynamic)
          """
        )
        assertTrue(
          errors.nonEmpty,
          errors.exists(_.message.contains("must be a constant string literal")),
        )
      },
      test("externalTable declares an ExternalTable node from a qualified id") {
        val fragment = externalTable("main.bronze.orders")
        assertTrue(
          fragment == GraphFragment(
            List(PipelineNode.ExternalTable("main.bronze.orders")),
            Set.empty,
          )
        )
      },
      test("a materialized view reading an external table validates (not dangling)") {
        val ext  = externalTable("main.bronze.orders")
        val gold = materializedView("gold") {
          spark.table("main.bronze.orders").select(col("id"))
        }
        val program = ManifestAssembly
          .assemble(List(ext, gold))
          .provide(ManifestAssembly.live, GraphValidation.live)
        program.either.map(r => assertTrue(r.isRight))
      },
    ),
    suite("streamingTable (F6)")(
      test("extracts the declared node and one edge per upstream reference") {
        val fragment = streamingTable("silver_orders") { ctx =>
          ctx.readStream.table("bronze_orders")
        }
        assertTrue(
          fragment == GraphFragment(
            List(PipelineNode.StreamingTable("silver_orders", "delta")),
            Set(DependencyEdge("bronze_orders", "silver_orders")),
          )
        )
      },
      test("finds upstream references through intermediate vals and joins") {
        val fragment = streamingTable("gold_orders") { ctx =>
          val stream = ctx.readStream.table("silver_orders")
          val dim    = ctx.table("dim_customers")
          stream.join(dim)
        }
        assertTrue(
          fragment.edges == Set(
            DependencyEdge("silver_orders", "gold_orders"),
            DependencyEdge("dim_customers", "gold_orders"),
          )
        )
      },
      test("deduplicates repeated upstream references") {
        val fragment = streamingTable("agg") { ctx =>
          ctx.table("src").join(ctx.table("src"))
        }
        assertTrue(fragment.edges == Set(DependencyEdge("src", "agg")))
      },
      test("rejects a non-literal upstream reference at compile time") {
        val errors = typeCheckErrors(
          """
          import dev.sdp.dsl.*
          val upstream: String = "runtime_value"
          val bad = streamingTable("target") { ctx => ctx.readStream.table(upstream) }
          """
        )
        assertTrue(
          errors.nonEmpty,
          errors.exists(_.message.contains("must be a constant string literal")),
        )
      },
    ),
    suite("materializedView / temporaryView (F9b)")(
      test("extracts name and SQL into the matching node") {
        val mv = materializedView("daily")("SELECT day, sum(x) FROM orders GROUP BY day")
        val tv = temporaryView("base")("SELECT id FROM range(1, 100)")
        assertTrue(
          mv == GraphFragment(
            List(PipelineNode.MaterializedView("daily", "SELECT day, sum(x) FROM orders GROUP BY day")),
            Set.empty,
          ),
          tv == GraphFragment(
            List(PipelineNode.TemporaryView("base", "SELECT id FROM range(1, 100)")),
            Set.empty,
          ),
        )
      },
      test("sqlStreamingTable carries an authored SQL flow in the fragment") {
        import dev.sdp.core.algebra.Rel
        val fragment = sqlStreamingTable("silver")("SELECT * FROM STREAM(bronze)")
        assertTrue(
          fragment.nodes == List(PipelineNode.StreamingTable("silver", "delta")),
          fragment.flows == List(Flow("silver", "silver", Rel.Sql("SELECT * FROM STREAM(bronze)"))),
        )
      },
      test("an authored flow survives the embed round-trip byte-for-byte") {
        val fragment = sqlStreamingTable("rt")("SELECT 1 AS one")
        assertTrue(
          GraphFragment.parse(GraphFragment.render(fragment)) == Right(fragment)
        )
      },
      test("rejects non-literal SQL at compile time, reporting all errors at once") {
        val errors = typeCheckErrors(
          """
          import dev.sdp.dsl.*
          val dyn: String = "x"
          val bad = materializedView(dyn)(dyn)
          """
        )
        assertTrue(
          errors.count(_.message.contains("must be a constant string literal")) == 2
        )
      },
    ),
    suite("end to end (F6 demo)")(
      test("Scala source in, validated canonical manifest out — no Spark anywhere") {
        val bronze = table("bronze_orders")
        val silver = streamingTable("silver_orders") { ctx =>
          ctx.readStream.table("bronze_orders")
        }
        val gold = streamingTable("gold_orders") { ctx =>
          ctx.readStream.table("silver_orders").join(ctx.table("dim_customers"))
        }
        val dims = table("dim_customers")

        for manifest <- ManifestAssembly.assemble(List(bronze, silver, gold, dims))
        yield assertTrue(
          manifest.nodes.map(_.id) ==
            List("bronze_orders", "dim_customers", "gold_orders", "silver_orders"),
          manifest.edges == List(
            DependencyEdge("bronze_orders", "silver_orders"),
            DependencyEdge("dim_customers", "gold_orders"),
            DependencyEdge("silver_orders", "gold_orders"),
          ),
          manifest.toGraph.topologicalSort.isRight,
        )
      },
      test("a cycle written in the DSL fails validation with a readable path") {
        val a = streamingTable("a") { ctx => ctx.readStream.table("b") }
        val b = streamingTable("b") { ctx => ctx.readStream.table("a") }

        for exit <- ManifestAssembly.assemble(List(a, b)).exit
        yield assertTrue(
          exit.causeOption.flatMap(_.failureOption).exists(_.contains(
            PipelineValidationError.CycleDetected(List("a", "b", "a"))
          ))
        )
      },
    ),
  ).provide(ManifestAssembly.live, GraphValidation.live)
