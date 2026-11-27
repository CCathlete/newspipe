package newspipe



import com.sun.net.httpserver.HttpServer

import newspipe.domain.models.ProbeResponse

import newspipe.infrastructure.source.JavaHttpFeedProbe

import org.scalatest.funsuite.AnyFunSuite



import java.net.InetSocketAddress

import java.nio.charset.StandardCharsets

import java.time.Duration

import java.util.concurrent.atomic.AtomicInteger



final class FeedProbeSpec extends AnyFunSuite:

  private def withServer(handler: (String, String) => (Int, String, String))(test: Int => Unit): Unit =

    val server: HttpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)

    val calls: AtomicInteger = new AtomicInteger(0)

    server.createContext("/", exchange => {

      calls.incrementAndGet()

      val result: (Int, String, String) = handler(exchange.getRequestURI.toString, exchange.getRequestHeaders.getFirst("User-Agent"))

      exchange.getResponseHeaders.add("Content-Type", result._2)

      val bytes: Array[Byte] = result._3.getBytes(StandardCharsets.UTF_8)

      exchange.sendResponseHeaders(result._1, bytes.length.toLong)

      exchange.getResponseBody.write(bytes)

      exchange.close()

    })

    server.start()

    try test(server.getAddress.getPort)

    finally server.stop(0)



  test("probe captures status, redirect target and content type") {

    withServer((path, agent) =>

      if path == "/start" then (302, "text/plain", "")

      else if path == "/feed" then (200, "text/plain", "body") else (404, "text/html", "missing")

    ) { port =>

      val probe: JavaHttpFeedProbe = new JavaHttpFeedProbe(Duration.ofSeconds(2))

      val ok: Either[String, ProbeResponse] = probe.probe("http://127.0.0.1:" + port + "/feed")

      assert(ok.isRight)

      assert(ok.toOption.get.statusCode == 200)

      assert(ok.toOption.get.contentType == "text/plain")

      val error: Either[String, ProbeResponse] = probe.probe("http://127.0.0.1:" + port + "/gone")

      assert(error.toOption.get.statusCode == 404)

    }

  }



  test("probe deduplicates repeated URLs inside one instance") {

    withServer((_, _) => (200, "application/rss+xml", "body")) { port =>

      val probe: JavaHttpFeedProbe = new JavaHttpFeedProbe(Duration.ofSeconds(2))

      val url: String = "http://127.0.0.1:" + port + "/rss"

      probe.probe(url)

      probe.probe(url)

      assert(probe.probe(url).isRight)

    }

  }



  test("probe returns a Left on connection refusal") {

    val probe: JavaHttpFeedProbe = new JavaHttpFeedProbe(Duration.ofSeconds(1))

    assert(probe.probe("http://127.0.0.1:1/refused").isLeft)

  }
