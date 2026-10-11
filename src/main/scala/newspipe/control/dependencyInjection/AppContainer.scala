package newspipe.control.dependencyInjection

import newspipe.application.services.IngestionService
import newspipe.infrastructure.bronze.DeltaBronzeSink
import newspipe.infrastructure.llmdataenricher.OpenCodeCliClient
import newspipe.infrastructure.env.EnvConfig
import newspipe.infrastructure.messagequeue.KafkaRawPublisher
import newspipe.infrastructure.rss.RomeRssClient
import org.apache.spark.sql.SparkSession

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

final class AppContainer(env: EnvConfig):
  val spark: SparkSession = SparkSession.builder()
    .appName("newspipe")
    .master(env.sparkMaster)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
  val rss = new RomeRssClient(env.feeds)
  val enrich = new OpenCodeCliClient(env.opencodeModel, FiniteDuration(env.opencodeTimeoutSeconds, TimeUnit.SECONDS))
  val sink = new DeltaBronzeSink(spark, env.bronzePath)
  val raw = new KafkaRawPublisher(env.kafkaBootstrap, env.rawTopic)
  val service = new IngestionService(rss, enrich, sink, raw)

  def close(): Unit =
    raw.close()
    spark.stop()
