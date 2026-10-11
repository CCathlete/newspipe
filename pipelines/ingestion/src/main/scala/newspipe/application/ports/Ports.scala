package newspipe.application.ports

import newspipe.domain.models.{BronzeUnit, RssItem}

trait RssSource:
  def fetch(): List[RssItem]

trait BronzeSink:
  def write(units: List[BronzeUnit]): Int

trait RawPublisher:
  def publish(items: List[RssItem]): Unit
