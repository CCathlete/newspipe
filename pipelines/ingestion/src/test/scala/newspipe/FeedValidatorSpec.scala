package newspipe



import newspipe.domain.models.ProbeResponse

import newspipe.domain.models.SourceCandidate

import newspipe.domain.models.ValidationStatus

import newspipe.infrastructure.source.RomeFeedValidator

import org.scalatest.funsuite.AnyFunSuite



final class FeedValidatorSpec extends AnyFunSuite:

  private val validator: RomeFeedValidator = new RomeFeedValidator

  private val candidate: SourceCandidate = SourceCandidate("pub", None, None, List("http://feed"))

  private val rss: String = "<?xml version=\"1.0\"?><rss version=\"2.0\"><channel><title>T</title><link>http://feed</link><description>D</description><item><title>I</title><link>http://feed/1</link><guid>http://feed/1</guid><description>Body</description><pubDate>Tue, 26 Nov 2024 10:00:00 GMT</pubDate></item></channel></rss>"

  private val atom: String = "<?xml version=\"1.0\"?><feed xmlns=\"http://www.w3.org/2005/Atom\"><title>T</title><link href=\"http://feed\"/><updated>2024-11-26T10:00:00Z</updated><entry><title>I</title><id>http://feed/1</id><link href=\"http://feed/1\"/><updated>2024-11-26T10:00:00Z</updated><summary>Body</summary></entry></feed>"

  private val rdf: String = "<?xml version=\"1.0\"?><rdf:RDF xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"><channel rdf:about=\"http://feed\"><title>T</title></channel></rdf:RDF>"

  private val discovery: String = "<html><head><link rel=\"alternate\" type=\"application/rss+xml\" href=\"http://feed/rss\"></head></html>"



  private def check(body: String, contentType: String): ValidationStatus =

    val response: ProbeResponse = ProbeResponse(200, "http://feed", contentType, body)

    validator.validate(candidate, response, 1732610400000L).status



  test("RSS2 and Atom documents are usable") {

    assert(check(rss, "application/rss+xml") == ValidationStatus.Usable)

    assert(check(atom, "application/atom+xml") == ValidationStatus.Usable)

  }



  test("RSS1 RDF is identified as needing investigation") {

    assert(check(rdf, "application/rdf+xml") == ValidationStatus.NeedsInvestigation)

    assert(validator.discover(discovery).contains("http://feed/rss"))

  }



  test("HTML without discovery and empty or malformed bodies are invalid") {

    assert(check("<html><body>page</body></html>", "text/html") == ValidationStatus.Invalid)

    assert(check("", "text/plain") == ValidationStatus.Invalid)

    assert(check("<rss><channel><item>", "application/rss+xml") == ValidationStatus.Invalid)

  }



  test("HTTP 503 is temporary while 404 is invalid") {

    val temp: ProbeResponse = ProbeResponse(503, "http://feed", "text/plain", "")

    val dead: ProbeResponse = ProbeResponse(404, "http://feed", "text/plain", "")

    assert(validator.validate(candidate, temp, 0L).status == ValidationStatus.TemporarilyUnavailable)

    assert(validator.validate(candidate, dead, 0L).status == ValidationStatus.Invalid)

  }
