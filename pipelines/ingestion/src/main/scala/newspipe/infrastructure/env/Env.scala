package newspipe.infrastructure.env

import com.typesafe.config.{Config, ConfigFactory}

import scala.jdk.CollectionConverters.*

final case class EnvConfig(
    feeds: List[String],
    pollIntervalSeconds: Long,
    kafkaBootstrap: String,
    rawTopic: String,
    bronzePath: String,
    sparkMaster: String
)

object Env:
  def load(): EnvConfig =
    load(ConfigFactory.load().getConfig("newspipe"))

  def load(cfg: Config): EnvConfig =
    EnvConfig(
      feeds = sys.env.get("NEWSPIPE_FEEDS").map(_.split(",").toList.map(_.trim).filter(_.nonEmpty))
        .getOrElse(cfg.getStringList("feeds").asScala.toList),
      pollIntervalSeconds = sys.env.get("NEWSPIPE_POLL_INTERVAL_SECONDS").map(_.toLong)
        .getOrElse(cfg.getLong("poll-interval-seconds")),
      kafkaBootstrap = sys.env.get("NEWSPIPE_KAFKA_BOOTSTRAP")
        .getOrElse(cfg.getString("kafka.bootstrap-servers")),
      rawTopic = sys.env.get("NEWSPIPE_RAW_TOPIC")
        .getOrElse(cfg.getString("kafka.raw-topic")),
      bronzePath = sys.env.get("NEWSPIPE_BRONZE_PATH")
        .getOrElse(cfg.getString("bronze.path")),
      sparkMaster = sys.env.get("NEWSPIPE_SPARK_MASTER")
        .getOrElse(cfg.getString("spark.master"))
    )
