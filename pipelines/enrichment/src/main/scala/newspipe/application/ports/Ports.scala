package newspipe.application.ports

import newspipe.domain.models.{BronzeUnit, Enrichment, RssItem}

trait RssSource:
  def fetch(): List[RssItem]

trait EnrichmentPort:
  def enrich(item: RssItem): Either[Throwable, Enrichment]

trait BronzeSink:
  def write(units: List[BronzeUnit]): Int

trait RawPublisher:
  def publish(items: List[RssItem]): Unit

trait RawConsumer:
  def poll(): List[RssItem]
  def commit(): Unit

trait BronzeMergeSink:
  def merge(units: List[BronzeUnit]): Int
