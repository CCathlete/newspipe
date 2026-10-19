package newspipe.infrastructure.env

import com.typesafe.config.Config
import newspipe.infrastructure.conf.ConfLoader

import java.nio.file.Paths
import scala.jdk.CollectionConverters.*

final case class EnvConfig(
    feeds: List[String],
    pollIntervalSeconds: Long,
    kafkaBootstrap: String,
    rawTopic: String,
    bronzePath: String,
    sparkMaster: String,
    s3Endpoint: String,
    s3PathStyle: Boolean,
    s3AccessKey: String,
    s3SecretKey: String,
    s3Bucket: String
)

object Env:
  private val PipelineFile = "ingestion.conf"

  def load(): Either[String, EnvConfig] =
    ConfLoader.load(PipelineFile, Paths.get("").toAbsolutePath).map: loaded =>
      val dot: DotEnv = DotEnv.discover(Paths.get("").toAbsolutePath)
      load(loaded.config.getConfig("newspipe"), dot)

  def load(cfg: Config): EnvConfig =
    load(cfg, DotEnv.discover(Paths.get("").toAbsolutePath))

  def load(cfg: Config, dot: DotEnv): EnvConfig =
    def pick(key: String, fallback: => String): String =
      sys.env.get(key).orElse(dot.get(key)).getOrElse(fallback)
    def pickLong(key: String, fallback: => Long): Long =
      sys.env.get(key).map(_.toLong).orElse(dot.get(key).map(_.toLong)).getOrElse(fallback)
    def pickBool(key: String, fallback: => Boolean): Boolean =
      sys.env.get(key).map(_.toBoolean).orElse(dot.get(key).map(_.toBoolean)).getOrElse(fallback)

    EnvConfig(
      feeds = pick("NEWSPIPE_FEEDS", cfg.getStringList("feeds").asScala.mkString(","))
        .split(",").toList.map(_.trim).filter(_.nonEmpty),
      pollIntervalSeconds = pickLong("NEWSPIPE_POLL_INTERVAL_SECONDS", cfg.getLong("poll-interval-seconds")),
      kafkaBootstrap = pick("NEWSPIPE_KAFKA_BOOTSTRAP", cfg.getString("kafka.bootstrap-servers")),
      rawTopic = pick("NEWSPIPE_RAW_TOPIC", cfg.getString("kafka.raw-topic")),
      bronzePath = pick("NEWSPIPE_BRONZE_PATH", cfg.getString("bronze.path")),
      sparkMaster = pick("NEWSPIPE_SPARK_MASTER", cfg.getString("spark.master")),
      s3Endpoint = pick("NEWSPIPE_S3_ENDPOINT", cfg.getString("s3.endpoint")),
      s3PathStyle = pickBool("NEWSPIPE_S3_PATH_STYLE", cfg.getBoolean("s3.path-style-access")),
      s3AccessKey = pick("NEWSPIPE_S3_ACCESS_KEY", cfg.getString("s3.access-key")),
      s3SecretKey = pick("NEWSPIPE_S3_SECRET_KEY", cfg.getString("s3.secret-key")),
      s3Bucket = pick("NEWSPIPE_BRONZE_BUCKET", cfg.getString("s3.bucket"))
    )
