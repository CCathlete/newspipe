package newspipe.application.services

import com.typesafe.scalalogging.LazyLogging
import newspipe.application.ports.{BronzeMergeSink, EnrichmentPort, RawConsumer}
import newspipe.domain.models.{BronzeUnit, Enrichment, RssItem}

import java.time.Instant
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import scala.annotation.tailrec
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future}

/** What one poll produced: rows merged, items enriched, items dropped. */
final case class PollReport(merged: Int, enriched: Int, failed: Int)

final class EnrichmentService(
    consumer: RawConsumer,
    enrich: EnrichmentPort,
    sink: BronzeMergeSink,
    models: List[String],
    workers: Int,
    maxAttempts: Int
) extends LazyLogging:
  private val RetryPauseMillis: Long = 2000L
  private val chain: List[String] =
    val configured: List[String] = models.map(_.trim).filter(_.nonEmpty).distinct
    if configured.isEmpty then throw new IllegalArgumentException("no enrichment model configured")
    else configured
  private val pool: ExecutorService = Executors.newFixedThreadPool(math.max(1, workers))
  private given ExecutionContext = ExecutionContext.fromExecutorService(pool)

  def close(): Unit =
    pool.shutdown()
    pool.awaitTermination(30, TimeUnit.SECONDS)
    ()

  /** Batches the poll across at most `workers` threads. Await uses Duration.Inf
    * because every task is bounded: maxAttempts times the per-item timeout. */
  def runOnce(now: Instant): PollReport =
    val items = consumer.poll()
    val futures: List[Future[Either[Throwable, Enrichment]]] =
      items.map(item => Future(attempt(item)))
    val results: List[Either[Throwable, Enrichment]] =
      Await.result(Future.sequence(futures), Duration.Inf)
    val paired = items.zip(results)
    val units: List[BronzeUnit] = paired.collect { case (it, Right(en)) => BronzeUnit.fromRss(it, en, now) }
    val failed: Int = paired.count(_._2.isLeft)
    val merged: Int = sink.merge(units)
    consumer.commit()
    PollReport(merged, units.size, failed)

  private def attempt(item: RssItem): Either[Throwable, Enrichment] =
    val total: Int = math.max(1, maxAttempts)
    @tailrec
    def loop(remaining: Int): Either[Throwable, Enrichment] =
      val used: Int = total - remaining
      val attemptNo: Int = used + 1
      val model: String = chain(used % chain.size)
      enrich.enrich(item, model) match
        case Right(en) => Right(en)
        case Left(err) if remaining > 1 =>
          logger.warn(s"enrichment attempt $attemptNo of $total failed for ${item.link} with $model: ${err.getMessage}")
          Thread.sleep(RetryPauseMillis)
          loop(remaining - 1)
        case Left(err) => Left(err)
    loop(total)