package dev.sdp.connect

import java.net.URI
import java.nio.file.{Files, Path, Paths, StandardCopyOption}

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
  * trying a candidate build. A suite that needs more than a stock distribution
  * asks for a [[Flavor]] instead — extra server-side jars and `--conf` entries
  * (see [[Iceberg42]]).
  */
object SparkConnectTestServer:

  final case class Server(host: String, port: Int)

  /** A jar the SERVER needs and the distribution does not ship — fixture
    * material, never a build dependency: it is mounted into the container and
    * handed to `start-connect-server.sh` via `--jars`. It never touches our
    * classpath, and `build.sbt` must never learn about it.
    *
    * `--jars` (a mounted file) rather than `--packages` (an Ivy resolve inside
    * the container): the download happens once on the HOST, into a cache
    * outside the repo, so repeated runs are offline and the exact bytes under
    * test are pinned by URL. */
  final case class ServerJar(url: String):
    /** The file name is the URL's last segment — which is why a snapshot pin
      * must name the TIMESTAMPED artifact: `…-1.12.0-SNAPSHOT.jar` is a moving
      * target both in the cache and upstream. */
    def fileName: String = url.substring(url.lastIndexOf('/') + 1)

  /** How to start a server: an image, plus anything the distribution lacks. */
  final case class Flavor(
      image: String,
      jars: List[ServerJar] = Nil,
      confs: List[(String, String)] = Nil,
  )

  /** Where fetched fixture jars live — deliberately OUTSIDE the repo (a 48 MB
    * jar is not source), shared across runs, overridable for CI. */
  val CacheDir: String =
    sys.env.getOrElse("SDP_IT_CACHE", s"${sys.props("user.home")}/.cache/sdp-it")

  /** Mount point for [[CacheDir]] inside the container. */
  private val JarMount = "/opt/sdp-jars"

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

  /** The Iceberg flavor of the 4.2 server — the only OSS way to get an AUTO CDC
    * target that Spark will actually merge into.
    *
    * `FlowExecution.requireDestinationSupportsRowLevelOps` refuses an AUTO CDC
    * flow unless the target's V2 table implements `SupportsRowLevelOperations`.
    * Measured 2026-09-12: parquet (V1) and Delta 4.4.0 are both refused; Iceberg's
    * `SparkTable` DOES implement it (read off the jar below:
    * `SparkTable … implements SupportsRead, SupportsWrite, SupportsDeleteV2,
    * SupportsRowLevelOperations`) but Iceberg has published **no Spark 4.2
    * release** — Central carries `iceberg-spark-runtime-4.0/4.1` only.
    *
    * So the pin is an Apache SNAPSHOT, and it names the **timestamped** artifact
    * on purpose: `1.12.0-SNAPSHOT` is mutable — the same coordinate serves
    * different bytes tomorrow, which would make this suite's verdict
    * irreproducible. Build `-20260912.002841-8` is the one these rows were
    * measured against. `scripts/upstream-watch.sh` watches Central for the
    * 1.12.0 RELEASE; when it lands, swap this one line.
    *
    * The confs mirror the proven June demo stack, minus Nessie: the Iceberg
    * extensions, `SparkSessionCatalog` over the session catalog (so ordinary
    * parquet fixtures still work through the fallback), a hadoop catalog on a
    * container-local warehouse, and `spark.sql.sources.default=iceberg` — that
    * last one is what makes an SDP target Iceberg **without the client sending a
    * format** (D13: the catalog default does the work). */
  val Iceberg42: Flavor = Flavor(
    image = Spark42Image,
    jars = List(
      ServerJar(
        "https://repository.apache.org/content/repositories/snapshots/org/apache/iceberg/" +
          "iceberg-spark-runtime-4.2_2.13/1.12.0-SNAPSHOT/" +
          "iceberg-spark-runtime-4.2_2.13-1.12.0-20260912.002841-8.jar"
      )
    ),
    confs = List(
      "spark.sql.extensions"                     -> "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      "spark.sql.catalog.spark_catalog"          -> "org.apache.iceberg.spark.SparkSessionCatalog",
      "spark.sql.catalog.spark_catalog.type"     -> "hadoop",
      "spark.sql.catalog.spark_catalog.warehouse" -> "/tmp/iceberg-warehouse",
      "spark.sql.sources.default"                -> "iceberg",
    ),
  )

  /** The default server: [[DefaultImage]], or whatever `SDP_SPARK_IMAGE` names.
    * The env override applies to [[layerFor]] too, so one variable re-points
    * every gated suite at a candidate server (that is how a Spark upgrade gets
    * smoke-tested before anything is pinned). */
  val layer: ZLayer[Any, Throwable, Server] = layerFor(DefaultImage)

  /** A server on a specific image — for suites that require a *particular*
    * server generation (AUTO CDC needs 4.2+, so `AutoCdcE2eSpec` asks for
    * [[Spark42Image]] rather than inheriting the 4.1 oracle pin). */
  def layerFor(image: String): ZLayer[Any, Throwable, Server] = layerFor(Flavor(image))

  /** A server with extra server-side jars and confs. */
  def layerFor(flavor: Flavor): ZLayer[Any, Throwable, Server] =
    ZLayer.scoped(start(flavor.copy(image = sys.env.getOrElse("SDP_SPARK_IMAGE", flavor.image))))

  private def start(flavor: Flavor): ZIO[Scope, Throwable, Server] =
    for
      cli  <- detectCli
      _    <- ZIO.foreachDiscard(flavor.jars)(fetch)
      name <- Random.nextUUID.map(u => s"sdp-it-$u")
      _ <- ZIO.acquireRelease(
        ZIO.attemptBlocking {
          // Mount the whole cache read-only (one bind, any number of jars) and
          // hand the container-side paths to spark-submit. In client mode
          // `--jars` lands on the driver's own classloader, which is where
          // `spark.sql.extensions` and the catalog class are resolved from.
          val mount   = if flavor.jars.isEmpty then Nil else List("-v", s"$CacheDir:$JarMount:ro")
          val jarArgs =
            if flavor.jars.isEmpty then Nil
            else List("--jars", flavor.jars.map(j => s"$JarMount/${j.fileName}").mkString(","))
          val confArgs = flavor.confs.flatMap((k, v) => List("--conf", s"$k=$v"))
          val cmd = List(
            cli, "run", "-d", "--name", name,
            "-p", "127.0.0.1::15002", // random host port, loopback only
            "-e", "SPARK_NO_DAEMONIZE=1",
          ) ++ mount ++ List(
            flavor.image,
            "/opt/spark/sbin/start-connect-server.sh",
          ) ++ jarArgs ++ confArgs
          val out = cmd.!!
          require(out.trim.nonEmpty, s"container failed to start: $out")
        }
      )(_ => ZIO.attemptBlocking(List(cli, "rm", "-f", name).!!).ignoreLogged)
      _ <- awaitReady(cli, name)
        .timeoutFail(new RuntimeException(s"${flavor.image} not ready within 120s"))(120.seconds)
      port <- mappedPort(cli, name)
    yield Server("127.0.0.1", port)

  /** Download a fixture jar into [[CacheDir]] once. A gated, opt-in suite may
    * reach the network; a green offline run must never depend on it, which is
    * why the file is kept (and re-used) outside the build's target dirs. */
  private def fetch(jar: ServerJar): Task[Unit] =
    ZIO
      .attemptBlocking {
        val dir: Path = Paths.get(CacheDir)
        val _         = Files.createDirectories(dir)
        val target    = dir.resolve(jar.fileName)
        if !Files.exists(target) || Files.size(target) == 0 then
          // Download to a sibling temp file and move: a half-written jar in the
          // cache would poison every later run with a ClassNotFound.
          val tmp = Files.createTempFile(dir, jar.fileName, ".part")
          try
            val in = URI.create(jar.url).toURL.openStream()
            try { val _ = Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING) }
            finally in.close()
            val _ = Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING)
          finally { val _ = Files.deleteIfExists(tmp) }
      }
      .mapError(e =>
        new RuntimeException(
          s"could not fetch the server-side fixture jar ${jar.url} into $CacheDir (${e.getMessage}). " +
            "This suite is opt-in (SDP_INTEGRATION) and downloads it ONCE; if this machine is offline, " +
            s"fetch it by hand: curl -sSfL -o $CacheDir/${jar.fileName} ${jar.url}",
          e,
        )
      )

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
