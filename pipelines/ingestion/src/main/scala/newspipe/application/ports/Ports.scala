package newspipe.application.ports

import newspipe.domain.models.{BronzeUnit, FeedValidation, ProbeResponse, RssItem, SourceCandidate, ValidationSnapshot}

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

trait SourceManifestReader:
  def read(path: String): Either[String, List[SourceCandidate]]

trait FeedProbe:
  def probe(url: String): Either[String, ProbeResponse]

trait FeedValidator:
  def validate(candidate: SourceCandidate, response: ProbeResponse, nowMillis: Long): FeedValidation
  def discover(body: String): Option[String]

trait ValidationResultStore:
  def load(): Either[String, Option[ValidationSnapshot]]
  def save(snapshot: ValidationSnapshot): Either[String, Unit]
