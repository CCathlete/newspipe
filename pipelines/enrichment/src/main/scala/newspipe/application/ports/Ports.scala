package newspipe.application.ports

import newspipe.domain.models.{BronzeUnit, Enrichment, RssItem}

trait RssSource:
  def fetch(): List[RssItem]

trait EnrichmentPort:
  def enrich(item: RssItem, model: String): Either[Throwable, Enrichment]

trait BronzeSink:
  def write(units: List[BronzeUnit]): Int

trait RawPublisher:
  def publish(items: List[RssItem]): Unit

trait RawConsumer:
  def poll(): List[RssItem]
  def commit(): Unit

trait BronzeMergeSink:
  def merge(units: List[BronzeUnit]): Int

sealed trait ProbeResult

object ProbeResult:
  case object Ready extends ProbeResult
  final case class NotReady(dependency: String, detail: String) extends ProbeResult

trait DependencyProbe:
  def name: String
  def check(): ProbeResult

trait KafkaReadinessProbe extends DependencyProbe

trait ObjectStoreReadinessProbe extends DependencyProbe

trait CliReadinessProbe extends DependencyProbe
