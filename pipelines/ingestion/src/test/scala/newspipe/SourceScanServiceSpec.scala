package newspipe


import newspipe.application.ports.FeedProbe

import newspipe.application.ports.FeedValidator

import newspipe.application.ports.SourceManifestReader

import newspipe.application.ports.ValidationResultStore

import newspipe.application.services.ScanReport

import newspipe.application.services.SourceScanService

import newspipe.domain.models.ProbeResponse

import newspipe.domain.models.SourceCandidate

import newspipe.domain.models.SourceValidationModels.given

import newspipe.domain.models.ValidationSnapshot

import newspipe.infrastructure.source.RomeFeedValidator

import org.scalatest.funsuite.AnyFunSuite


import java.util.concurrent.atomic.AtomicInteger


final class SourceScanServiceSpec extends AnyFunSuite:

  private val rss: String = "<?xml version=\"1.0\"?><rss version=\"2.0\"><channel><title>T</title><link>http://f</link><description>D</description><item><title>I</title><link>http://f/1</link><guid>http://f/1</guid><description>Body</description><pubDate>Tue, 26 Nov 2024 10:00:00 GMT</pubDate></item></channel></rss>"



  private final class MemoryReader(candidates: List[SourceCandidate]) extends SourceManifestReader:

    def read(path: String): Either[String, List[SourceCandidate]] = Right(candidates)



  private final class MemoryStore(var snapshot: Option[ValidationSnapshot]) extends ValidationResultStore:

    def load(): Either[String, Option[ValidationSnapshot]] = Right(snapshot)

    def save(value: ValidationSnapshot): Either[String, Unit] =

      snapshot = Some(value)

      Right(())



  private final class CountingProbe extends FeedProbe:

    val calls: AtomicInteger = new AtomicInteger(0)

    def probe(url: String): Either[String, ProbeResponse] =

      calls.incrementAndGet()

      if url.contains("broken") then Left("connection refused: " + url)

      else if url.contains("missing") then Right(ProbeResponse(404, url, "text/html", ""))

      else Right(ProbeResponse(200, url, "application/rss+xml", rss))



  test("one broken endpoint does not stop the batch and statuses are counted") {

    val candidates: List[SourceCandidate] = List(

      SourceCandidate("good", None, None, List("http://f/good")),

      SourceCandidate("down", None, None, List("http://f/broken")),

      SourceCandidate("dead", None, None, List("http://f/missing"))

    )

    val store: MemoryStore = new MemoryStore(None)

    val service: SourceScanService = new SourceScanService(new MemoryReader(candidates),

      new CountingProbe, new RomeFeedValidator, store, "unused.json", 2, false)

    val result: Either[String, ScanReport] = service.runOnce()

    assert(result.isRight)

    val report: ScanReport = result.toOption.get

    assert(report.candidates == 3)

    assert(report.usable == 1)

    assert(report.unavailable == 1)

    assert(report.invalid == 1)

    assert(report.usableUrls == List("http://f/good"))

    service.close()

  }



  test("an unchanged re-run reuses a usable result without re-probing") {

    val candidates: List[SourceCandidate] = List(SourceCandidate("good", None, None, List("http://f/good")))

    val probe: CountingProbe = new CountingProbe

    val store: MemoryStore = new MemoryStore(None)

    val service: SourceScanService = new SourceScanService(new MemoryReader(candidates),

      probe, new RomeFeedValidator, store, "unused.json", 2, false)

    assert(service.runOnce().isRight)

    assert(probe.calls.get() == 1)

    assert(service.runOnce().toOption.get.usable == 1)

    assert(probe.calls.get() == 1)

    service.close()

  }