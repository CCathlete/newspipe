package newspipe.application

import com.typesafe.scalalogging.LazyLogging
import newspipe.domain.*

import java.time.Instant
import scala.collection.mutable

final class IngestionService(
    rss: RssSource,
    enrich: EnrichmentPort,
    sink: BronzeSink,
    raw: RawPublisher
) extends LazyLogging:
  private val seen = mutable.HashSet.empty[String]

  def runOnce(now: Instant): Int =
    val items = rss.fetch()
    raw.publish(items)
    val fresh = items.filter(it => seen.add(BronzeUnit.idFor(it)))
    val units = fresh.map { it =>
      val e = enrich.enrich(it) match
        case Right(en) => en
        case Left(err) =>
          logger.warn(s"enrichment failed for ${it.link}: ${err.getMessage}; using empty enrichment")
          Enrichment.empty
      BronzeUnit.fromRss(it, e, now)
    }
    if units.isEmpty then 0 else sink.write(units)
