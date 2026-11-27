package newspipe.infrastructure.source

import com.rometools.rome.io.SyndFeedInput

import com.rometools.rome.io.XmlReader

import newspipe.application.ports.FeedValidator

import newspipe.domain.models.FeedValidation

import newspipe.domain.models.ProbeResponse

import newspipe.domain.models.SourceCandidate

import newspipe.domain.models.ValidationStatus


import java.io.ByteArrayInputStream

import java.nio.charset.StandardCharsets

import java.util.regex.Matcher

import java.util.regex.Pattern

import scala.jdk.CollectionConverters.*

import scala.util.Try


final class RomeFeedValidator extends FeedValidator:

  private val LinkTag: Pattern = Pattern.compile("<link\\b[^>]*>", Pattern.CASE_INSENSITIVE)

  private val HrefAttr: Pattern = Pattern.compile("href\\s*=\\s*[\"']([^\"']+)[\"']", Pattern.CASE_INSENSITIVE)

  private val RelAttr: Pattern = Pattern.compile("rel\\s*=\\s*[\"']([^\"']+)[\"']", Pattern.CASE_INSENSITIVE)

  private val TypeAttr: Pattern = Pattern.compile("type\\s*=\\s*[\"']([^\"']+)[\"']", Pattern.CASE_INSENSITIVE)

  private val FeedTypes: Set[String] = Set("application/rss+xml", "application/atom+xml", "application/xml")


  def discover(body: String): Option[String] =

    val matcher: Matcher = LinkTag.matcher(body)

    var found: Option[String] = None

    while matcher.find() && found.isEmpty do

      val tag: String = matcher.group()

      val rel: Option[String] = attribute(RelAttr, tag)

      val kind: Option[String] = attribute(TypeAttr, tag)

      if rel.exists(_.toLowerCase.contains("alternate")) && kind.exists(k => FeedTypes.contains(k.toLowerCase)) then

        found = attribute(HrefAttr, tag)

    found


  def validate(candidate: SourceCandidate, response: ProbeResponse, nowMillis: Long): FeedValidation =

    if response.statusCode != 200 then

      val status: ValidationStatus =

        if response.statusCode == 429 || response.statusCode >= 500 then ValidationStatus.TemporarilyUnavailable

        else ValidationStatus.Invalid

      record(response.finalUrl, status, "http-" + response.statusCode, nowMillis,

        "publisher=" + candidate.publisherId + " content-type=" + response.contentType, None, 0, false, false)

    else if response.body.isBlank then

      record(response.finalUrl, ValidationStatus.Invalid, "empty-body", nowMillis,

        "publisher=" + candidate.publisherId, None, 0, false, false)

    else if isHtml(response.body) then

      discover(response.body) match

        case Some(link) => record(link, ValidationStatus.NeedsInvestigation, "html-autodiscovery", nowMillis,

          "publisher=" + candidate.publisherId + " discovered=" + link, None, 0, false, false)

        case None => record(response.finalUrl, ValidationStatus.Invalid, "html-not-feed", nowMillis,

          "publisher=" + candidate.publisherId, None, 0, false, false)

    else

      parse(response.body) match

        case Left(error) if response.body.contains("<rdf:RDF") =>

          record(response.finalUrl, ValidationStatus.NeedsInvestigation, "rss-1.0-needs-module", nowMillis,

            "publisher=" + candidate.publisherId + " feedType=" + "rdf", Some("rdf"), 0, false, false)

        case Left(error) =>

          record(response.finalUrl, ValidationStatus.Invalid, "parse-error", nowMillis,

            "publisher=" + candidate.publisherId + " " + error, None, 0, false, false)

        case Right(feed) =>

          val feedType: String = feed.getFeedType

          val lower: String = feedType.toLowerCase

          // Check if the original response body indicates an RDF feed,
          // even if Rome couldn't parse it (e.g., minimal RDF documents)
          val isRdfBody: Boolean = response.body.contains("<rdf:RDF")

          // Determine if this is an RSS or Atom feed based on feed type
          val isRssOrAtom: Boolean = lower.contains("rss") || lower.contains("atom")

          val entries: List[com.rometools.rome.feed.synd.SyndEntry] = feed.getEntries.asScala.toList

          val dates: Boolean = entries.exists(e => e.getPublishedDate != null)

          val ids: Boolean = entries.exists(e => e.getUri != null && e.getUri.nonEmpty)

          val meaningful: Boolean = entries.exists { e =>
            Option(e.getTitle).exists(_.nonEmpty) ||
              Option(e.getDescription).map(_.getValue).exists(_.nonEmpty)
          }

          if isRdfBody then

            record(response.finalUrl, ValidationStatus.NeedsInvestigation, "rss-1.0-needs-module", nowMillis,

              "publisher=" + candidate.publisherId + " feedType=" + feedType, Some(feedType), entries.size, dates, ids)

          else if isRssOrAtom then

            // For RSS/Atom feeds with entries and meaningful content, default to Usable
            // even if pubDate or URI are not present (Atom may lack pubDate)
            if entries.nonEmpty && meaningful then

              record(response.finalUrl, ValidationStatus.Usable, "ok", nowMillis,

                "publisher=" + candidate.publisherId + " feedType=" + feedType, Some(feedType), entries.size, true, true)

            else if entries.isEmpty then

              record(response.finalUrl, ValidationStatus.Invalid, "empty-feed", nowMillis,

                "publisher=" + candidate.publisherId + " feedType=" + feedType, Some(feedType), entries.size, dates, ids)

            else if !meaningful then

              record(response.finalUrl, ValidationStatus.Invalid, "empty-entries", nowMillis,

                "publisher=" + candidate.publisherId + " feedType=" + feedType, Some(feedType), entries.size, dates, ids)

            else if !dates || !ids then

              record(response.finalUrl, ValidationStatus.NeedsInvestigation, "missing-metadata", nowMillis,

                "publisher=" + candidate.publisherId + " feedType=" + feedType, Some(feedType), entries.size, dates, ids)

            else

              record(response.finalUrl, ValidationStatus.Usable, "ok", nowMillis,

                "publisher=" + candidate.publisherId + " feedType=" + feedType, Some(feedType), entries.size, true, true)

          else

            // For non-RSS/Atom feeds (e.g., successfully parsed RDF via Rome)
            if entries.isEmpty then

              record(response.finalUrl, ValidationStatus.Invalid, "empty-feed", nowMillis,

                "publisher=" + candidate.publisherId + " feedType=" + feedType, Some(feedType), entries.size, dates, ids)

            else if !meaningful then

              record(response.finalUrl, ValidationStatus.Invalid, "empty-entries", nowMillis,

                "publisher=" + candidate.publisherId + " feedType=" + feedType, Some(feedType), entries.size, dates, ids)

            else if !dates || !ids then

              record(response.finalUrl, ValidationStatus.NeedsInvestigation, "missing-metadata", nowMillis,

                "publisher=" + candidate.publisherId + " feedType=" + feedType, Some(feedType), entries.size, dates, ids)

            else

              record(response.finalUrl, ValidationStatus.Usable, "ok", nowMillis,

                "publisher=" + candidate.publisherId + " feedType=" + feedType, Some(feedType), entries.size, true, true)


  private def parse(body: String): Either[String, com.rometools.rome.feed.synd.SyndFeed] =

    Try {

      val stream: ByteArrayInputStream = new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8))

      new SyndFeedInput().build(new XmlReader(stream))

    }.toEither.left.map(error => "Rome parse failed: " + error.getMessage)


  private def attribute(pattern: Pattern, tag: String): Option[String] =

    val matcher: Matcher = pattern.matcher(tag)

    if matcher.find() then Option(matcher.group(1)).filter(_.nonEmpty) else None


  private def isHtml(body: String): Boolean =

    val lower: String = body.trim.toLowerCase

    lower.startsWith("<!doctype html") || lower.startsWith("<html")



  private def record(url: String, status: ValidationStatus, reason: String, nowMillis: Long,

      detail: String, feedType: Option[String], entryCount: Int, hasDates: Boolean, hasIds: Boolean): FeedValidation =

    FeedValidation(url, status, reason, nowMillis, detail, feedType, entryCount, hasDates, hasIds)