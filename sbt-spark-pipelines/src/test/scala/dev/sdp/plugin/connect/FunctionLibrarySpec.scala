package dev.sdp.plugin.connect

import dev.sdp.core.algebra.*
import dev.sdp.dsl.*
import dev.sdp.plugin.AlgebraProtoEncoder
import zio.*
import zio.test.*

import PlanAnalysis.SchemaField

/** The pass-through bet, tested: `fn(name, args*)` claims the *entire* Spark
  * SQL function library by sending unresolved functions for the server's
  * analyzer to bind. This battery covers string, math, conditional and
  * temporal functions — authored in the **fluent flow language**, extracted
  * by the macro, and schema-checked by live Catalyst. If the analyzer binds
  * the name and agrees on the type, the function works; there is no
  * client-side registry to fall out of date.
  */
object FunctionLibrarySpec extends ZIOSpecDefault:

  private val enabled =
    sys.env.contains("SDP_INTEGRATION") || java.lang.Boolean.getBoolean("sdp.integration")

  /** The probe pipeline is authored exactly as an end user would write it;
    * we then lift the macro-extracted relation straight into AnalyzePlan.
    */
  private val probe: Rel =
    materializedViewFrom("fn_probe") {
      val base = read.table("ignored").select(
        // strings
        fn("length", lit("hello")).as("len"),
        fn("upper", lit("shout")).as("up"),
        fn("concat", lit("a"), lit("b"), lit("c")).as("cat"),
        fn("substring", lit("abcdef"), lit(2), lit(3)).as("sub"),
        // math
        fn("abs", lit(-42)).as("absolute"),
        fn("sqrt", lit(144)).as("root"),
        fn("pow", lit(2), lit(10)).as("powed"),
        fn("round", fn("sqrt", lit(2)), lit(3)).as("rounded"),
        (lit(7) % lit(3)).as("modded"),
        // conditional / null handling
        fn("coalesce", lit(1), lit(2)).as("first_present"),
        fn("greatest", lit(1), lit(9), lit(5)).as("biggest"),
        // temporal
        fn("current_date").as("today"),
        fn("date_add", fn("current_date"), lit(7)).as("next_week"),
      )
      base
    }.flows.head.relation match
      // swap the placeholder read for a self-contained leaf the analyzer can resolve
      case Rel.Project(_, columns) => Rel.Project(Rel.Sql("SELECT 1 AS one"), columns)
      case other                   => other

  def spec =
    val tests = suite("Spark SQL function library pass-through (fn)")(
      test("string, math, conditional and temporal functions all bind and type-check") {
        for
          server <- ZIO.service[SparkConnectTestServer.Server]
          fields <- PlanAnalysis.analyzeSchema(server.host, server.port, AlgebraProtoEncoder.relation(probe))
        yield assertTrue(
          fields == List(
            SchemaField("len", "integer"),
            SchemaField("up", "string"),
            SchemaField("cat", "string"),
            SchemaField("sub", "string"),
            SchemaField("absolute", "integer"),
            SchemaField("root", "double"),
            SchemaField("powed", "double"),
            SchemaField("rounded", "double"),
            SchemaField("modded", "integer"),
            SchemaField("first_present", "integer"),
            SchemaField("biggest", "integer"),
            SchemaField("today", "date"),
            SchemaField("next_week", "date"),
          )
        )
      },
      test("a misspelled function is a typed server rejection naming the function") {
        val bad = Rel.Project(Rel.Sql("SELECT 1 AS one"), List(Ex.Fn("not_a_function", List(Ex.Lit(LitValue.I32(1))))))
        for
          server <- ZIO.service[SparkConnectTestServer.Server]
          exit   <- PlanAnalysis.analyzeSchema(server.host, server.port, AlgebraProtoEncoder.relation(bad)).exit
        yield assertTrue(
          exit.causeOption.flatMap(_.failureOption).exists {
            case PipelinesRegistration.RegistrationError.ServerRejected(d) =>
              d.contains("not_a_function")
            case _ => false
          }
        )
      },
    ).provideShared(SparkConnectTestServer.layer) @@ TestAspect.withLiveEnvironment
      @@ TestAspect.sequential @@ TestAspect.timeout(5.minutes)

    if enabled then tests else tests @@ TestAspect.ignore
