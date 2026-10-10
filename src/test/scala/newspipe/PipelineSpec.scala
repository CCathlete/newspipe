// E2E (code lead runs in verify): `grep -rli crawl4ai src/main/scala` must be empty
// E2E (code lead runs in verify): `grep -rli litellm src/main/scala` must be empty
// E2E (code lead runs in verify): `grep -rn "^import" src/main/scala/newspipe/domain/` must show no infrastructure/kafka/spark/delta/rome imports
package newspipe

import newspipe.application.IngestionService
import newspipe.domain.*
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class PipelineSpec extends AnyFunSpec with Matchers:
  private def item(g: String) = RssItem(g, "https://feed/x", s"T$g", s"https://a/$g", None, "Body")
  describe("IngestionService.runOnce") {
    it("publishes raw, enriches, writes bronze, dedups second run, falls back on CLI failure") {
      var published: List[RssItem] = Nil
      var written: List[BronzeUnit] = Nil
      val rss = new RssSource:
        def fetch(): List[RssItem] = List(item("g1"), item("g2"))
      val raw = new RawPublisher:
        def publish(items: List[RssItem]): Unit = published = items
      val enrich = new EnrichmentPort:
        def enrich(i: RssItem): Either[Throwable, Enrichment] =
          if i.guid == "g2" then Left(new RuntimeException("boom")) else Right(Enrichment("S", Nil, "Global", Nil, Nil, "low", "en"))
      val sink = new BronzeSink:
        def write(units: List[BronzeUnit]): Int =
          written = units
          units.size
      val svc = new IngestionService(rss, enrich, sink, raw)
      val now = Instant.parse("2026-10-10T10:00:00Z")
      svc.runOnce(now) shouldBe 2
      published.map(_.guid) shouldBe List("g1", "g2")
      written.map(_.summary) shouldBe List("S", "")
      svc.runOnce(now) shouldBe 0
    }
  }
