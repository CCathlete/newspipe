package newspipe.application.ports

import newspipe.domain.models.{BronzeUnit, RssItem}

trait RssSource:
  def fetch(): List[RssItem]

trait BronzeSink:
  def write(units: List[BronzeUnit]): Int

trait RawPublisher:
  def publish(items: List[RssItem]): Unit

sealed trait ProbeResult

object ProbeResult:
  case object Ready extends ProbeResult
  final case class NotReady(dependency: String, detail: String) extends ProbeResult

trait DependencyProbe:
  def name: String
  def check(): ProbeResult

trait KafkaReadinessProbe extends DependencyProbe

trait ObjectStoreReadinessProbe extends DependencyProbe
