package newspipe.infrastructure

import com.rometools.rome.io.{SyndFeedInput, XmlReader}
import newspipe.domain.{RssItem, RssSource}

import java.io.ByteArrayInputStream
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import java.time.{Duration, Instant}
import scala.jdk.CollectionConverters.*
import scala.util.Try

final class RomeRssClient(feedUrls: List[String], timeout: Duration = Duration.ofSeconds(20))
    extends RssSource:

  private val http: HttpClient =
    HttpClient.newBuilder.connectTimeout(timeout).followRedirects(HttpClient.Redirect.NORMAL).build()

  def fetch(): List[RssItem] =
    feedUrls.flatMap(fetchFeed)

  private def fetchFeed(feedUrl: String): List[RssItem] =
    fetchXml(feedUrl).toList.flatMap(parse(_, feedUrl))

  private def fetchXml(feedUrl: String): Option[String] =
    Try {
      val req = HttpRequest.newBuilder(URI.create(feedUrl))
        .timeout(timeout)
        .header("User-Agent", "newspipe-scala/0.2")
        .GET()
        .build()
      val res = http.send(req, HttpResponse.BodyHandlers.ofString())
      if res.statusCode() == 200 then Some(res.body()) else None
    }.toOption.flatten

  private def parse(xml: String, feedUrl: String): List[RssItem] =
    Try {
      val stream = new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8))
      val feed = new SyndFeedInput().build(new XmlReader(stream))
      feed.getEntries.asScala.toList.map { e =>
        RssItem(
          guid = Option(e.getUri).filter(_.nonEmpty).getOrElse(Option(e.getLink).getOrElse("")),
          feedUrl = feedUrl,
          title = Option(e.getTitle).getOrElse(""),
          link = Option(e.getLink).getOrElse(""),
          publishedAt = Option(e.getPublishedDate).map(d => Instant.ofEpochMilli(d.getTime)),
          description = Option(e.getDescription).map(_.getValue).getOrElse("")
        )
      }
    }.toOption.getOrElse(Nil)
