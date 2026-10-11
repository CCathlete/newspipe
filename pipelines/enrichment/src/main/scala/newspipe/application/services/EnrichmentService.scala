package newspipe.application.services

import com.typesafe.scalalogging.LazyLogging
import newspipe.application.ports.{BronzeMergeSink, EnrichmentPort, RawConsumer}
import newspipe.domain.models.BronzeUnit
import java.time.Instant

final class EnrichmentService(
    consumer: RawConsumer,
    enrich: EnrichmentPort,
    sink: BronzeMergeSink
) extends LazyLogging:
  def runOnce(now: Instant): Int =
    val items = consumer.poll()
    val merged = items.flatMap { it =>
      enrich.enrich(it) match
        case Right(en) => Some(BronzeUnit.fromRss(it, en, now))
        case Left(err) =>
          logger.warn(s"enrichment failed for ${it.link}: ${err.getMessage}; leaving unenriched")
          None
    }
    val n = sink.merge(merged)
    consumer.commit()
    n
