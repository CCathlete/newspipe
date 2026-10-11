package newspipe.domain.models

import java.time.Instant

final case class RssItem(
    guid: String,
    feedUrl: String,
    title: String,
    link: String,
    publishedAt: Option[Instant],
    description: String
)
