package newspipe.control.dependencyInjection

import newspipe.application.services.{EnrichmentService, ReadinessGate}
import newspipe.infrastructure.bronze.{DeltaMergeSink, HttpObjectStoreReadinessProbe}
import newspipe.infrastructure.env.EnvConfig
import newspipe.infrastructure.kafka.{KafkaRawConsumer, TcpKafkaReadinessProbe}
import newspipe.infrastructure.llmdataenricher.{OpenCodeCliClient, OpenCodeCliReadinessProbe}
import org.apache.spark.sql.SparkSession

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*

final class AppContainer(env: EnvConfig):
  val spark: SparkSession = SparkSession.builder()
    .appName("newspipe-enrichment")
    .master(env.sparkMaster)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", env.s3Endpoint)
    .config("spark.hadoop.fs.s3a.path.style.access", env.s3PathStyle.toString)
    .config("spark.hadoop.fs.s3a.access.key", env.s3AccessKey)
    .config("spark.hadoop.fs.s3a.secret.key", env.s3SecretKey)
    .getOrCreate()
  val consumer = new KafkaRawConsumer(env.kafkaBootstrap, env.rawTopic, env.groupId)
  val enrich = new OpenCodeCliClient(env.opencodeModel, FiniteDuration(env.opencodeTimeoutSeconds, TimeUnit.SECONDS))
  val sink = new DeltaMergeSink(spark, env.bronzePath)
  val service = new EnrichmentService(consumer, enrich, sink)
  val readiness: ReadinessGate = new ReadinessGate(
    List(
      new TcpKafkaReadinessProbe(env.kafkaBootstrap, 3000),
      new HttpObjectStoreReadinessProbe(env.s3Endpoint, env.s3Bucket, 3000),
      new OpenCodeCliReadinessProbe("opencode")
    ),
    maxWait = 60.seconds,
    interval = 2.seconds,
    sleep = d => Thread.sleep(d.toMillis)
  )

  def close(): Unit =
    consumer.close()
    spark.stop()
