package dev.sdp.dsl

import scala.compiletime.testing.{typeCheckErrors, typeChecks}

import dev.sdp.core.*
import dev.sdp.core.algebra.*
import zio.test.*

/** F14a spike verdict: typed column references over named-tuple schemas.
  *
  * Kill criterion 1 (load-bearing): `c.amount` must extract to the same
  * tree as `col("amount")`. Criterion: wrong columns are *type* errors with
  * readable messages. (Compile-time budget is measured separately.)
  */
object TypedColsSpec extends ZIOSpecDefault:

  type Orders = (order_id: Long, amount: Long, customer_name: String)

  def spec = suite("typed columns (F14a spike)")(
    test("c.amount extracts to exactly the tree col(\"amount\") produces") {
      val typed = streamingTableFrom("typed") {
        val c = cols[Orders]
        stream.table("orders").where(c.amount > lit(0L)).select(c.order_id, c.amount)
      }
      val stringly = streamingTableFrom("typed") {
        stream.table("orders").where(col("amount") > lit(0L)).select(col("order_id"), col("amount"))
      }
      assertTrue(typed.flows.head.relation == stringly.flows.head.relation)
    },
    test("typed refs compose with operators, aliases and functions") {
      val fragment = streamingTableFrom("composed") {
        val c = cols[Orders]
        stream.table("orders").select(
          (c.amount * lit(2L)).as("doubled"),
          fn("upper", c.customer_name).as("shout"),
        )
      }
      assertTrue(
        fragment.flows.head.relation == Rel.Project(
          Rel.NamedTable("orders", streaming = true),
          List(
            Ex.Alias(Ex.Fn("*", List(Ex.Col("amount"), Ex.Lit(LitValue.I64(2L)))), "doubled"),
            Ex.Alias(Ex.Fn("upper", List(Ex.Col("customer_name"))), "shout"),
          ),
        )
      )
    },
    test("a wrong column is a TYPE error before extraction even runs") {
      val errors = typeCheckErrors(
        """
        import dev.sdp.dsl.*
        type Orders = (order_id: Long, amount: Long)
        val bad = streamingTableFrom("x") {
          val c = cols[Orders]
          stream.table("orders").where(c.amout > lit(0L))
        }
        """
      )
      assertTrue(
        errors.nonEmpty,
        // the compiler names the missing member — readable, localized
        errors.exists(e => e.message.contains("amout") && !e.message.contains("match type")),
      )
    },
    test("correct columns type-check cleanly (no false positives)") {
      assertTrue(typeChecks(
        """
        import dev.sdp.dsl.*
        type Orders = (order_id: Long, amount: Long)
        val ok = streamingTableFrom("x") {
          val c = cols[Orders]
          stream.table("orders").select(c.order_id, c.amount)
        }
        """
      ))
    },
  )
