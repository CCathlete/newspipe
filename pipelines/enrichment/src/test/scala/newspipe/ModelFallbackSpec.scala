package newspipe

import newspipe.application.ports.*
import newspipe.application.services.EnrichmentService
import newspipe.domain.models.*
import newspipe.domain.valueObjects.{GeoArea, Urgency}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class ModelFallbackSpec extends AnyFunSpec with Matchers:
  private def item(g: String): RssItem = RssItem(g, "https://feed/x", s"T$g", s"https://a/$g", None, "Body")

  describe("EnrichmentService model fallback") {
    it("rotates from the primary to the fallback within maxAttempts") {
      var calls: List[String] = Nil
      val consumer = new RawConsumer:
        def poll(): List[RssItem] = List(item("g1"))
        def commit(): Unit = ()
      val port = new EnrichmentPort:
        def enrich(i: RssItem, model: String): Either[Throwable, Enrichment] =
          calls = calls :+ model
          if model == "primary" then Left(new RuntimeException("primary down"))
          else Right(Enrichment("S", Nil, GeoArea("Global"), Nil, Nil, Urgency("low"), "en"))
      val sink = new BronzeMergeSink:
        def merge(units: List[BronzeUnit]): Int = units.size
      val report = new EnrichmentService(consumer, port, sink, List("primary", "fallback"), 1, 2)
        .runOnce(Instant.parse("2026-10-29T00:00:00Z"))
      report.enriched shouldBe 1
      report.failed shouldBe 0
      calls shouldBe List("primary", "fallback")
    }
  }