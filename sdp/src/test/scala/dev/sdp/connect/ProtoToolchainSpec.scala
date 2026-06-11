package dev.sdp.connect

import org.apache.spark.connect.proto as sc
import zio.test.*

/** F8a toolchain proof: Spark's generated protobuf classes are consumable
  * from Scala 3 and round-trip through protobuf-java. Field numbers and
  * message names here were read from the canonical
  * `../spark/sql/connect/common/src/main/protobuf/spark/connect/pipelines.proto`.
  */
object ProtoToolchainSpec extends ZIOSpecDefault:

  def spec = suite("Spark Connect proto toolchain (F8a)")(
    test("PipelineCommand with DefineOutput round-trips through bytes") {
      val cmd = sc.PipelineCommand
        .newBuilder()
        .setDefineOutput(
          sc.PipelineCommand.DefineOutput
            .newBuilder()
            .setDataflowGraphId("graph-1")
            .setOutputName("bronze_orders")
            .setOutputType(sc.OutputType.TABLE)
            .setTableDetails(
              sc.PipelineCommand.DefineOutput.TableDetails
                .newBuilder()
                .setFormat("delta")
            )
        )
        .build()

      val parsed = sc.PipelineCommand.parseFrom(cmd.toByteArray)
      assertTrue(
        parsed.hasDefineOutput,
        parsed.getDefineOutput.getOutputName == "bronze_orders",
        parsed.getDefineOutput.getOutputType == sc.OutputType.TABLE,
        parsed.getDefineOutput.getTableDetails.getFormat == "delta",
      )
    },
    test("StartRun supports dry-run validation") {
      val cmd = sc.PipelineCommand
        .newBuilder()
        .setStartRun(
          sc.PipelineCommand.StartRun
            .newBuilder()
            .setDataflowGraphId("graph-1")
            .setDry(true)
        )
        .build()
      val parsed = sc.PipelineCommand.parseFrom(cmd.toByteArray)
      assertTrue(parsed.getStartRun.getDry)
    },
  )
