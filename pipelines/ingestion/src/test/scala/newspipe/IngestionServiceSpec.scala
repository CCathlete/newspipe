package newspipe

import newspipe.application.ports.*
import newspipe.application.services.IngestionService
import newspipe.domain.models.*
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class IngestionServiceSpec extends AnyFunSpec with Matchers:
  private def item(g: String) = RssItem(g, "https://feed/x", s"T$g", s"https://a/$g", None, "Body")

  describe("IngestionService.runOnce") {
    it("publishes raw and writes raw-only bronze rows, dedups second run") {
      var published: List[RssItem] = Nil
      var written: List[BronzeUnit] = Nil
      val svc = new IngestionService(
        new RssSource:
          def fetch(): List[RssItem] = List(item("g1"), item("g2")),
        new BronzeSink:
          def write(units: List[BronzeUnit]): Int =
            written = units
            units.size,
        new RawPublisher:
          def publish(items: List[RssItem]): Unit = published = items
      )
      val now = Instant.parse("2026-10-10T10:00:00Z")
      svc.runOnce(now) shouldBe 2
      published.map(_.guid) shouldBe List("g1", "g2")
      written.map(_.summary) shouldBe List("", "")
      svc.runOnce(now) shouldBe 0
    }
    it("retries the batch after a sink failure instead of losing it") {
      var calls = 0
      var written: List[BronzeUnit] = Nil
      val svc = new IngestionService(
        new RssSource:
          def fetch(): List[RssItem] = List(item("g1")),
        new BronzeSink:
          def write(units: List[BronzeUnit]): Int =
            calls += 1
            if calls == 1 then throw new RuntimeException("sink down")
            else
              written = units
              units.size,
        new RawPublisher:
          def publish(items: List[RssItem]): Unit = ()
      )
      val now = Instant.parse("2026-10-10T10:00:00Z")
      an[RuntimeException] should be thrownBy svc.runOnce(now)
      svc.runOnce(now) shouldBe 1
      written.map(_.id) shouldBe List(BronzeUnit.idFor(item("g1")))
      svc.runOnce(now) shouldBe 0
    }
  }
