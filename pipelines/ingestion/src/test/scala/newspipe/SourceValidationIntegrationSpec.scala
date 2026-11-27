package newspipe


import com.sun.net.httpserver.HttpServer

import newspipe.application.ports.FeedProbe

import newspipe.application.ports.FeedValidator

import newspipe.application.ports.SourceManifestReader

import newspipe.application.ports.ValidationResultStore

import newspipe.application.services.SourceScanService

import newspipe.domain.models.ProbeResponse

import newspipe.domain.models.SourceCandidate

import newspipe.domain.models.SourceValidationModels.given

import newspipe.domain.models.ValidationSnapshot

import newspipe.infrastructure.rss.ValidatedRssSource

import newspipe.infrastructure.source.RomeFeedValidator

import org.scalatest.funsuite.AnyFunSuite


import java.net.InetSocketAddress

import java.nio.charset.StandardCharsets

import java.time.Duration


final class SourceValidationIntegrationSpec extends AnyFunSuite:

  private val rss: String = "<?xml version=\"1.0\"?><rss version=\"2.0\"><channel><title>T</title><link>http://f</link><description>D</description><item><title>I</title><link>http://f/1</link><guid>http://f/1</guid><description>Body</description><pubDate>Tue, 26 Nov 2024 10:00:00 GMT</pubDate></item></channel></rss>"



  private final class SingleReader(url: String) extends SourceManifestReader:

    def read(path: String): Either[String, List[SourceCandidate]] =

      Right(List(SourceCandidate("pub", None, None, List(url))))



  private final class DirectProbe extends FeedProbe:

    def probe(url: String): Either[String, ProbeResponse] =

      Right(ProbeResponse(200, url, "application/rss+xml", rss))



  private final class MemoryStore extends ValidationResultStore:

    private var snapshot: Option[ValidationSnapshot] = None

    def load(): Either[String, Option[ValidationSnapshot]] = Right(snapshot)

    def save(value: ValidationSnapshot): Either[String, Unit] =

      snapshot = Some(value)

      Right(())



  test("scan result flows into the collection adapter and yields one item") {

    val server: HttpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)

    server.createContext("/rss", exchange => {

      val bytes: Array[Byte] = rss.getBytes(StandardCharsets.UTF_8)

      exchange.getResponseHeaders.add("Content-Type", "application/rss+xml")

      exchange.sendResponseHeaders(200, bytes.length.toLong)

      exchange.getResponseBody.write(bytes)

      exchange.close()

    })

    server.start()

    val url: String = "http://127.0.0.1:" + server.getAddress.getPort + "/rss"

    val service: SourceScanService = new SourceScanService(new SingleReader(url),

      new DirectProbe, new RomeFeedValidator, new MemoryStore, "unused.json", 1, false)

    val report = service.runOnce()

    assert(report.isRight)

    val usableUrls: List[String] = report.toOption.get.usableUrls

    assert(usableUrls == List(url))

    val source: ValidatedRssSource = new ValidatedRssSource(usableUrls, Duration.ofSeconds(2))

    assert(source.fetch().size == 1)

    service.close()

    server.stop(0)

  }