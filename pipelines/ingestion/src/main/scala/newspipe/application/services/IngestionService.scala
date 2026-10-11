package newspipe.application.services

import com.typesafe.scalalogging.LazyLogging
import newspipe.application.ports.*
import newspipe.domain.models.*

import java.time.Instant
import scala.collection.mutable

final class IngestionService(
    rss: RssSource,
    sink: BronzeSink,
    raw: RawPublisher
) extends LazyLogging:
  private val seen = mutable.HashSet.empty[String]

  def runOnce(now: Instant): Int =
    val items = rss.fetch()
    raw.publish(items)
    val fresh = items.filterNot(it => seen(BronzeUnit.idFor(it)))
    if fresh.isEmpty then 0
    else
      val units = fresh.map(it => BronzeUnit.fromRss(it, Enrichment.empty, now))
      val n = sink.write(units)
      units.foreach(u => seen.add(u.id))
      n
