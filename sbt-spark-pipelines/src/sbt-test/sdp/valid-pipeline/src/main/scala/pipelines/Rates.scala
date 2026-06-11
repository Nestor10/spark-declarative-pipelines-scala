package pipelines

import dev.sdp.core.GraphFragment
import dev.sdp.dsl.*

object Rates:
  /** Schema declared at the source leaf; everything downstream is inferred —
    * and `sdpImportSchemas` turns the inferred shapes into named-tuple aliases
    * for `cols[...]`. (Inference runs over the evaluated flow's algebra tree.)
    */
  val rates: GraphFragment = streamingTable("rates") {
    spark.readStream
      .format("rate")
      .schema("timestamp TIMESTAMP, value BIGINT")
      .load()
      .select(col("value"), col("timestamp").as("seen_at"))
  }