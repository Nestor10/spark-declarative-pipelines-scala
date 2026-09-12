package dev.sdp.connect

import scala.sys.process.*

import zio.*

/** A Spark Connect server container for integration tests, driven through
  * the `podman`/`docker` CLI (whichever is on PATH — they accept the same
  * arguments).
  *
  * Deliberately NOT Testcontainers: this project's dev environment is
  * Podman, where Testcontainers needs socket plumbing (`DOCKER_HOST`) and
  * Ryuk workarounds. Shelling to the CLI is engine-neutral, adds zero
  * dependencies, and the lifecycle is a textbook scoped resource: acquire
  * starts the container and awaits readiness; release force-removes it —
  * guaranteed, even when tests die (Zionomicon ch. 14/15, ch. 44 for the
  * suite-shared layer).
  *
  * Facts verified by the F9a spike (2026-06-06): `apache/spark:4.1.2` ships
  * `spark-pipelines_2.13` in its distribution; `start-connect-server.sh`
  * with `SPARK_NO_DAEMONIZE=1` runs in the foreground; readiness is the log
  * line "Spark Connect server started". All three still hold on 4.2.0
  * (verified 2026-09-12).
  *
  * The image is a **parameter**: suites that need a specific server generation
  * call [[layerFor]], and `SDP_SPARK_IMAGE` overrides every suite at once for
  * trying a candidate build.
  */
object SparkConnectTestServer:

  final case class Server(host: String, port: Int)

  /** The conformance-oracle pin: everything that only needs *analysis* runs
    * here, and the drift-gated inventory is rendered against this era. */
  val DefaultImage = "docker.io/apache/spark:4.1.2"

  /** 4.2.0 (released, not master): the first server that speaks AUTO CDC. */
  val Spark42Image = "docker.io/apache/spark:4.2.0"

  /** Readiness mark — verified unchanged on 4.2.0, where the line reads
    * "Spark Connect server started at: [::]:15002" (the prefix is the stable
    * part). It is on **stderr**, which is why [[awaitReady]] captures both
    * streams (D3, learned the hard way). */
  private val ReadyMark = "Spark Connect server started"

  /** The default server: [[DefaultImage]], or whatever `SDP_SPARK_IMAGE` names.
    * The env override applies to [[layerFor]] too, so one variable re-points
    * every gated suite at a candidate server (that is how a Spark upgrade gets
    * smoke-tested before anything is pinned). */
  val layer: ZLayer[Any, Throwable, Server] = layerFor(DefaultImage)

  /** A server on a specific image — for suites that require a *particular*
    * server generation (AUTO CDC needs 4.2+, so `AutoCdcE2eSpec` asks for
    * [[Spark42Image]] rather than inheriting the 4.1 oracle pin). */
  def layerFor(image: String): ZLayer[Any, Throwable, Server] =
    ZLayer.scoped(start(sys.env.getOrElse("SDP_SPARK_IMAGE", image)))

  private def start(image: String): ZIO[Scope, Throwable, Server] =
    for
      cli  <- detectCli
      name <- Random.nextUUID.map(u => s"sdp-it-$u")
      _ <- ZIO.acquireRelease(
        ZIO.attemptBlocking {
          val cmd = List(
            cli, "run", "-d", "--name", name,
            "-p", "127.0.0.1::15002", // random host port, loopback only
            "-e", "SPARK_NO_DAEMONIZE=1",
            image,
            "/opt/spark/sbin/start-connect-server.sh",
          )
          val out = cmd.!!
          require(out.trim.nonEmpty, s"container failed to start: $out")
        }
      )(_ => ZIO.attemptBlocking(List(cli, "rm", "-f", name).!!).ignoreLogged)
      _    <- awaitReady(cli, name).timeoutFail(new RuntimeException(s"$image not ready within 120s"))(120.seconds)
      port <- mappedPort(cli, name)
    yield Server("127.0.0.1", port)

  private val detectCli: Task[String] =
    ZIO
      .attemptBlocking(List("podman", "docker").find(c => s"which $c".! == 0))
      .someOrFail(new RuntimeException("neither podman nor docker found on PATH"))

  private def awaitReady(cli: String, name: String): Task[Unit] =
    ZIO
      .attemptBlocking {
        // Spark logs to stderr; capture BOTH streams or the ready mark
        // never appears (found the hard way — a terminal shows both).
        val buf    = new StringBuilder
        val logger = ProcessLogger(s => { val _ = buf.append(s).append('\n') }, s => { val _ = buf.append(s).append('\n') })
        val _      = List(cli, "logs", name).!(logger)
        buf.result().contains(ReadyMark)
      }
      .repeat(Schedule.spaced(2.seconds).untilInput[Boolean](identity))
      .unit

  private def mappedPort(cli: String, name: String): Task[Int] =
    ZIO.attemptBlocking {
      // e.g. "15002/tcp -> 127.0.0.1:38731"
      val out = List(cli, "port", name, "15002").!!
      out.trim.split(':').last.toInt
    }
