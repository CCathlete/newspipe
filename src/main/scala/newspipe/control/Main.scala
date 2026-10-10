package newspipe.control

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import newspipe.application.IngestionService
import newspipe.infrastructure.{DeltaBronzeSink, KafkaRawPublisher, OpenCodeCliClient, RomeRssClient}
import org.apache.spark.sql.SparkSession

import java.time.{Duration, Instant}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.*

object Main extends LazyLogging:
  def main(args: Array[String]): Unit =
    val cfg = ConfigFactory.load().getConfig("newspipe")
    val feeds = cfg.getStringList("feeds").asScala.toList
    val spark = SparkSession.builder()
      .appName("newspipe")
      .master(cfg.getString("spark.master"))
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    val rss = new RomeRssClient(feeds)
    val enrich = new OpenCodeCliClient(
      cfg.getString("opencode.model"),
      FiniteDuration(cfg.getLong("opencode.timeout-seconds"), java.util.concurrent.TimeUnit.SECONDS)
    )
    val sink = new DeltaBronzeSink(spark, cfg.getString("bronze.path"))
    val raw = new KafkaRawPublisher(cfg.getString("kafka.bootstrap-servers"), cfg.getString("kafka.raw-topic"))
    val service = new IngestionService(rss, enrich, sink, raw)
    sys.addShutdownHook { raw.close(); spark.stop() }
    val poll = Duration.ofSeconds(cfg.getLong("poll-interval-seconds"))
    logger.info(s"newspipe polling ${feeds.size} feeds every $poll")
    while true do
      try
        val n = service.runOnce(Instant.now())
        logger.info(s"poll wrote $n bronze units")
      catch case e: Exception => logger.error("poll iteration failed", e)
      Thread.sleep(poll.toMillis)
