package dev.sdp.connect

import java.util.UUID
import java.util.concurrent.TimeUnit

import io.grpc.{ManagedChannelBuilder, StatusRuntimeException}
import org.apache.spark.connect.proto as sc
import zio.*

import PipelinesRegistration.RegistrationError

/** Runs plain SQL statements against the Spark Connect server — the local
  * **fixture** path: create + populate the source/catalog tables an
  * `externalTable` reads, so a full pipeline run resolves them.
  *
  * Why over Connect (not `spark-sql` inside the container): the connect server
  * owns the session catalog and metastore, so sending SQL on the *same* channel
  * sidesteps the embedded-derby single-process lock entirely (a second
  * `spark-sql` JVM can't open the same metastore while the server holds it). It
  * also reuses the transport everything else here already speaks.
  *
  * Each statement is an `ExecutePlanRequest` whose plan root is a `SQL`
  * relation; DDL/DML (CREATE/INSERT) execute eagerly server-side during
  * planning. This is NOT a pipeline command — the `Pipelines` Catalyst strategy
  * only blocks standalone execution of *pipeline* commands, not ordinary SQL.
  */
object CatalogSeeder:

  /** Execute each statement in order, sharing one session so temp state and
    * `USE`/`SET` carry across statements. Fails fast on the first rejection,
    * naming the offending statement. */
  def run(
      host: String,
      port: Int,
      statements: List[String],
  ): IO[RegistrationError, Unit] =
    ZIO.scoped {
      channel(host, port).flatMap { channel =>
        val stub      = sc.SparkConnectServiceGrpc.newBlockingStub(channel)
        val sessionId = UUID.randomUUID().toString
        ZIO.foreachDiscard(statements)(sql => exec(stub, sessionId, sql))
      }
    }

  private def exec(
      stub: sc.SparkConnectServiceGrpc.SparkConnectServiceBlockingStub,
      sessionId: String,
      sql: String,
  ): IO[RegistrationError, Unit] =
    ZIO
      .attemptBlocking {
        val request = sc.ExecutePlanRequest
          .newBuilder()
          .setSessionId(sessionId)
          .setUserContext(sc.UserContext.newBuilder().setUserId("sbt-spark-pipelines"))
          .setPlan(
            sc.Plan
              .newBuilder()
              .setRoot(sc.Relation.newBuilder().setSql(sc.SQL.newBuilder().setQuery(sql)))
          )
          .build()
        val it = stub.executePlan(request)
        while it.hasNext do { val _ = it.next() } // drain — eager DDL/DML runs here
      }
      .mapError {
        case e: StatusRuntimeException =>
          RegistrationError.ServerRejected(s"${e.getStatus.getCode}: ${e.getStatus.getDescription}\n  in: $sql")
        case other => RegistrationError.TransportFailure(s"$other\n  in: $sql")
      }

  private def channel(host: String, port: Int): ZIO[Scope, RegistrationError, io.grpc.ManagedChannel] =
    ZIO
      .acquireRelease(
        ZIO.attemptBlocking(ManagedChannelBuilder.forAddress(host, port).usePlaintext().build())
      )(ch =>
        ZIO.attemptBlocking {
          ch.shutdownNow()
          val _ = ch.awaitTermination(10, TimeUnit.SECONDS)
        }.orDie
      )
      .mapError(e => RegistrationError.TransportFailure(e.toString))
