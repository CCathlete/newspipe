package newspipe

import newspipe.application.ports.ProbeResult
import newspipe.infrastructure.bronze.HttpObjectStoreReadinessProbe
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}

import java.net.InetSocketAddress

class ObjectStoreReadinessProbeSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll:
  private var server: HttpServer = _
  private var lastAuth: String = null
  private var status: Int = 200

  private def start(): String =
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/health", new HttpHandler:
      def handle(ex: HttpExchange): Unit =
        lastAuth = ex.getRequestHeaders.getFirst("Authorization")
        val body: Array[Byte] = Array.emptyByteArray
        ex.sendResponseHeaders(status, body.length.toLong)
        ex.getResponseBody.write(body)
        ex.close()
    )
    server.start()
    s"http://127.0.0.1:${server.getAddress.getPort}"

  override def afterAll(): Unit = if server != null then server.stop(0)

  private def probe(endpoint: String): ProbeResult =
    new HttpObjectStoreReadinessProbe(endpoint, "lakehouse", 2000).check()

  describe("HttpObjectStoreReadinessProbe") {
    it("reports ready when the liveness endpoint answers 200") {
      status = 200
      probe(start()) shouldBe ProbeResult.Ready
    }
    it("sends no Authorization header at all") {
      status = 200
      probe(start())
      lastAuth shouldBe null
    }
    it("reports not-ready naming the endpoint when the liveness probe fails") {
      status = 503
      val result = probe(start())
      result match
        case ProbeResult.NotReady(dep, detail) =>
          dep shouldBe "object-store"
          detail should include("503")
        case other => fail(s"expected NotReady, got $other")
    }
    it("reports not-ready when the endpoint refuses the connection") {
      val result = probe("http://127.0.0.1:1")
      result shouldBe a[ProbeResult.NotReady]
    }
    it("reports not-ready when no endpoint is configured") {
      new HttpObjectStoreReadinessProbe("", "lakehouse", 2000).check() shouldBe
        ProbeResult.NotReady("object-store", "no endpoint configured")
    }
    it("reports not-ready when no bucket is configured") {
      new HttpObjectStoreReadinessProbe("http://127.0.0.1:1", "", 2000).check() shouldBe
        ProbeResult.NotReady("object-store", "no bucket configured")
    }
  }
