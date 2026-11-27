package newspipe.infrastructure.rss



import newspipe.application.ports.RssSource

import newspipe.domain.models.RssItem



import java.time.Duration



final class ValidatedRssSource(usableUrls: List[String], timeout: Duration = Duration.ofSeconds(20)) extends RssSource:

  private val delegate: RomeRssClient = new RomeRssClient(usableUrls, timeout)



  def fetch(): List[RssItem] = delegate.fetch()