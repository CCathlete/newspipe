package newspipe.control.dependencyInjection

import newspipe.application.services.IngestionService
import newspipe.infrastructure.bronze.DeltaBronzeSink
import newspipe.infrastructure.env.EnvConfig
import newspipe.infrastructure.messagequeue.KafkaRawPublisher
import newspipe.infrastructure.rss.RomeRssClient
import org.apache.spark.sql.SparkSession

final class AppContainer(env: EnvConfig):
  val spark: SparkSession = SparkSession.builder()
    .appName("newspipe-ingestion")
    .master(env.sparkMaster)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
  val rss = new RomeRssClient(env.feeds)
  val sink = new DeltaBronzeSink(spark, env.bronzePath)
  val raw = new KafkaRawPublisher(env.kafkaBootstrap, env.rawTopic)
  val service = new IngestionService(rss, sink, raw)

  def close(): Unit =
    raw.close()
    spark.stop()
