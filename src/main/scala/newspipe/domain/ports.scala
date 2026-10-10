package newspipe.domain

trait RssSource:
  def fetch(): List[RssItem]

trait EnrichmentPort:
  def enrich(item: RssItem): Either[Throwable, Enrichment]

trait BronzeSink:
  def write(units: List[BronzeUnit]): Int

trait RawPublisher:
  def publish(items: List[RssItem]): Unit
