package com.webcat.hotovo.business

import io.circe._
import io.circe.generic.semiauto._

case class ArticleToScrape(id: Int, url: String, source: String)

case class ExtractedAnalysis(
    headline: String,
    summary: String,
    geoEntities: List[String],
    biasScore: Double
)

object ExtractedAnalysis {
  implicit val decoder: Decoder[ExtractedAnalysis] = deriveDecoder
  implicit val encoder: Encoder[ExtractedAnalysis] = deriveEncoder
}

case class AnalyzedArticle(
    articleId: Int,
    url: String,
    source: String,
    headline: String,
    summary: String,
    geoEntities: List[String],
    biasScore: Double,
    ingestionTimestamp: Long
)

case class UrlToScrapeQuill(id: Int, url: String, source: String)
