package pipelines

import dev.sdp.core.GraphFragment
import dev.sdp.dsl.*

/** Two streaming tables whose flow bodies read each other: a -> b -> a.
  * The lineage comes from what the flows actually read — the build must
  * fail with the cycle path before anything reaches a server.
  */
object Cycle:
  val a: GraphFragment = streamingTable("table_a") {
    spark.readStream.table("table_b").select(col("x"))
  }
  val b: GraphFragment = streamingTable("table_b") {
    spark.readStream.table("table_a").select(col("x"))
  }
