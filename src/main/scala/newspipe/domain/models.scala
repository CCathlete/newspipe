package newspipe.domain

import java.time.Instant

final case class RssItem(
    guid: String,
    feedUrl: String,
    title: String,
    link: String,
    publishedAt: Option[Instant],
    description: String
)

final case class Enrichment(
    summary: String,
    countries: List[String],
    geoArea: String,
    topics: List[String],
    actors: List[String],
    urgency: String,
    language: String
)

object Enrichment:
  val empty: Enrichment =
    Enrichment("", Nil, "unknown", Nil, Nil, "low", "en")

final case class BronzeUnit(
    id: String,
    feedUrl: String,
    articleUrl: String,
    title: String,
    content: String,
    publishedAt: Option[Instant],
    ingestedAt: Instant,
    summary: String,
    countries: List[String],
    geoArea: String,
    topics: List[String],
    actors: List[String],
    urgency: String,
    language: String
)

object BronzeUnit:
  def fromRss(item: RssItem, enrichment: Enrichment, now: Instant): BronzeUnit =
    BronzeUnit(
      id = idFor(item),
      feedUrl = item.feedUrl,
      articleUrl = item.link,
      title = item.title,
      content = item.description,
      publishedAt = item.publishedAt,
      ingestedAt = now,
      summary = enrichment.summary,
      countries = enrichment.countries,
      geoArea = enrichment.geoArea,
      topics = enrichment.topics,
      actors = enrichment.actors,
      urgency = enrichment.urgency,
      language = enrichment.language
    )

  def idFor(item: RssItem): String =
    val base = if item.guid.nonEmpty then item.guid else item.link
    s"${item.feedUrl.hashCode.toHexString}-${Integer.toUnsignedString(base.hashCode)}"
