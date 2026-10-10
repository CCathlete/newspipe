package newspipe

import com.sun.net.httpserver.HttpServer
import newspipe.infrastructure.RomeRssClient
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.net.InetSocketAddress
import java.nio.file.{Files, Paths}
import java.util.concurrent.Executors

class RssClientSpec extends AnyFunSpec with Matchers:
  describe("RomeRssClient") {
    it("parses guid/title/link/date/description from a fixture feed") {
      val bytes = Files.readAllBytes(Paths.get("src/test/resources/fixture-feed.xml"))
      val server = HttpServer.create(new InetSocketAddress(0), 0)
      server.createContext(
        "/geo.xml",
        ex => {
          ex.getResponseHeaders.add("Content-Type", "application/rss+xml")
          ex.sendResponseHeaders(200, bytes.length)
          val os = ex.getResponseBody
          os.write(bytes)
          os.close()
        }
      )
      server.setExecutor(Executors.newSingleThreadExecutor())
      server.start()
      try
        val url = s"http://localhost:${server.getAddress.getPort}/geo.xml"
        val items = new RomeRssClient(List(url)).fetch()
        items should have size 2
        items.map(_.guid) shouldBe List("geo-1", "geo-2")
        items.head.title shouldBe "Summit on strait security"
        items.head.link shouldBe "https://example.com/geo/1"
        items.head.publishedAt.isDefined shouldBe true
        items.head.description should include("Naval drills")
        items.foreach(_.feedUrl shouldBe url)
      finally server.stop(0)
    }
  }
