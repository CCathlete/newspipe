package newspipe.control.dependencyInjection

import newspipe.application.services.IngestionService
import newspipe.application.services.ReadinessGate
import newspipe.application.services.ScanReport
import newspipe.application.services.SourceScanService
import newspipe.application.ports.FeedProbe
import newspipe.application.ports.FeedValidator
import newspipe.application.ports.SourceManifestReader
import newspipe.application.ports.ValidationResultStore
import newspipe.infrastructure.rss.ValidatedRssSource
import newspipe.infrastructure.source.JavaHttpFeedProbe
import newspipe.infrastructure.source.JsonSourceManifestReader
import newspipe.infrastructure.source.JsonValidationResultStore
import newspipe.infrastructure.source.RomeFeedValidator

import newspipe.infrastructure.bronze.{DeltaBronzeSink, HttpObjectStoreReadinessProbe}
import newspipe.infrastructure.env.EnvConfig
import newspipe.infrastructure.messagequeue.{KafkaRawPublisher, TcpKafkaReadinessProbe}
import newspipe.infrastructure.rss.RomeRssClient
import org.apache.spark.sql.SparkSession

import java.time.Duration

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
  val readiness: ReadinessGate = new ReadinessGate(
    List(
      new TcpKafkaReadinessProbe(env.kafkaBootstrap, 3000),
      new HttpObjectStoreReadinessProbe(env.s3Endpoint, env.s3Bucket, 3000)
    ),
    maxWait = 60.seconds,
    interval = 2.seconds,
    sleep = d => Thread.sleep(d.toMillis)
  )

  val manifestReader: SourceManifestReader = new JsonSourceManifestReader

  val feedProbe: FeedProbe = new JavaHttpFeedProbe(Duration.ofSeconds(env.probeTimeoutSeconds))

  val feedValidator: FeedValidator = new RomeFeedValidator

  val resultStore: ValidationResultStore = new JsonValidationResultStore(env.validationStorePath)

  val scanner: SourceScanService = new SourceScanService(

    manifestReader, feedProbe, feedValidator, resultStore,

    env.sourceManifestPath, env.scanWorkers, env.reprobeSources

  )


  def scan(): Either[String, ScanReport] = scanner.runOnce()


  def validatedService(usableUrls: List[String]): IngestionService =

    new IngestionService(new ValidatedRssSource(usableUrls), sink, raw)

  def close(): Unit =
    scanner.close()
    raw.close()
    spark.stop()