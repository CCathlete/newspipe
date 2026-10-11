package newspipe.control.dependencyInjection

import newspipe.application.services.EnrichmentService
import newspipe.infrastructure.bronze.DeltaMergeSink
import newspipe.infrastructure.env.EnvConfig
import newspipe.infrastructure.kafka.KafkaRawConsumer
import newspipe.infrastructure.llmdataenricher.OpenCodeCliClient
import org.apache.spark.sql.SparkSession

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

final class AppContainer(env: EnvConfig):
  val spark: SparkSession = SparkSession.builder()
    .appName("newspipe-enrichment")
    .master(env.sparkMaster)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
  val consumer = new KafkaRawConsumer(env.kafkaBootstrap, env.rawTopic, env.groupId)
  val enrich = new OpenCodeCliClient(env.opencodeModel, FiniteDuration(env.opencodeTimeoutSeconds, TimeUnit.SECONDS))
  val sink = new DeltaMergeSink(spark, env.bronzePath)
  val service = new EnrichmentService(consumer, enrich, sink)

  def close(): Unit =
    consumer.close()
    spark.stop()
