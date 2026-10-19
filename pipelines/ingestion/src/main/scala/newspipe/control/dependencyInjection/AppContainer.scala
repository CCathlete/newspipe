package newspipe.control.dependencyInjection

import newspipe.application.services.{IngestionService, ReadinessGate}
import newspipe.infrastructure.bronze.{DeltaBronzeSink, HttpObjectStoreReadinessProbe}
import newspipe.infrastructure.env.EnvConfig
import newspipe.infrastructure.messagequeue.{KafkaRawPublisher, TcpKafkaReadinessProbe}
import newspipe.infrastructure.rss.RomeRssClient
import org.apache.spark.sql.SparkSession

import scala.concurrent.duration.*

final class AppContainer(env: EnvConfig):
  val spark: SparkSession = SparkSession.builder()
    .appName("newspipe-ingestion")
    .master(env.sparkMaster)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", env.s3Endpoint)
    .config("spark.hadoop.fs.s3a.path.style.access", env.s3PathStyle.toString)
    .config("spark.hadoop.fs.s3a.access.key", env.s3AccessKey)
    .config("spark.hadoop.fs.s3a.secret.key", env.s3SecretKey)
    .getOrCreate()
  val rss = new RomeRssClient(env.feeds)
  val sink = new DeltaBronzeSink(spark, env.bronzePath)
  val raw = new KafkaRawPublisher(env.kafkaBootstrap, env.rawTopic)
  val service = new IngestionService(rss, sink, raw)
  val readiness: ReadinessGate = new ReadinessGate(
    List(
      new TcpKafkaReadinessProbe(env.kafkaBootstrap, 3000),
      new HttpObjectStoreReadinessProbe(env.s3Endpoint, env.s3Bucket, 3000)
    ),
    maxWait = 60.seconds,
    interval = 2.seconds,
    sleep = d => Thread.sleep(d.toMillis)
  )

  def close(): Unit =
    raw.close()
    spark.stop()
