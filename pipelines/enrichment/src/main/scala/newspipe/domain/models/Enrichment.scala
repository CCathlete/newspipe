package newspipe.domain.models

import newspipe.domain.valueObjects.{GeoArea, Urgency}

final case class Enrichment(
    summary: String,
    countries: List[String],
    geoArea: GeoArea,
    topics: List[String],
    actors: List[String],
    urgency: Urgency,
    language: String
)

object Enrichment:
  val empty: Enrichment =
    Enrichment("", Nil, GeoArea("unknown"), Nil, Nil, Urgency("low"), "en")
