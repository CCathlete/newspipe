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


final class SourceValidationScaleSpec extends AnyFunSuite:

  private val rss: String = "<?xml version=\"1.0\"?><rss version=\"2.0\"><channel><title>T</title><link>http://f</link><description>D</description><item><title>I</title><link>http://f/1</link><guid>http://f/1</guid><description>Body</description><pubDate>Tue, 26 Nov 2024 10:00:00 GMT</pubDate></item></channel></rss>"



  private final class ThousandReader extends SourceManifestReader:

    def read(path: String): Either[String, List[SourceCandidate]] =

      Right((1 to 1000).map(i => SourceCandidate("pub-" + i, None, None, List("http://f/" + i))).toList)



  private final class MemoryStore extends ValidationResultStore:

    private var snapshot: Option[ValidationSnapshot] = None

    def load(): Either[String, Option[ValidationSnapshot]] = Right(snapshot)

    def save(value: ValidationSnapshot): Either[String, Unit] =

      snapshot = Some(value)

      Right(())



  private final class BoundedProbe extends FeedProbe:

    val calls: AtomicInteger = new AtomicInteger(0)

    val active: AtomicInteger = new AtomicInteger(0)

    val peak: AtomicInteger = new AtomicInteger(0)

    def probe(url: String): Either[String, ProbeResponse] =

      calls.incrementAndGet()

      val running: Int = active.incrementAndGet()

      peak.accumulateAndGet(running, (a: Int, b: Int) => math.max(a, b))

      Thread.sleep(2L)

      active.decrementAndGet()

      Right(ProbeResponse(200, url, "application/rss+xml", rss))



  test("one thousand candidates are all accounted for with at most eight concurrent probes") {

    val probe: BoundedProbe = new BoundedProbe

    val service: SourceScanService = new SourceScanService(new ThousandReader,

      probe, new RomeFeedValidator, new MemoryStore, "unused.json", 8, false)

    val result: Either[String, ScanReport] = service.runOnce()

    assert(result.isRight)

    val report: ScanReport = result.toOption.get

    assert(report.candidates == 1000)

    assert(report.usable == 1000)

    assert(report.usableUrls.size == 1000)

    assert(probe.calls.get() == 1000)

    assert(probe.peak.get() <= 8)

    service.close()

  }