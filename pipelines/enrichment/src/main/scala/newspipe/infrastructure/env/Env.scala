package newspipe.infrastructure.env

import com.typesafe.config.{Config, ConfigFactory}

import scala.jdk.CollectionConverters.*

final case class EnvConfig(
    pollIntervalSeconds: Long,
    kafkaBootstrap: String,
    rawTopic: String,
    groupId: String,
    bronzePath: String,
    sparkMaster: String,
    opencodeModel: String,
    opencodeTimeoutSeconds: Long
)

object Env:
  def load(): EnvConfig =
    load(ConfigFactory.load().getConfig("newspipe"))

  def load(cfg: Config): EnvConfig =
    EnvConfig(
      pollIntervalSeconds = sys.env.get("NEWSPIPE_POLL_INTERVAL_SECONDS").map(_.toLong)
        .getOrElse(cfg.getLong("poll-interval-seconds")),
      kafkaBootstrap = sys.env.get("NEWSPIPE_KAFKA_BOOTSTRAP")
        .getOrElse(cfg.getString("kafka.bootstrap-servers")),
      rawTopic = sys.env.get("NEWSPIPE_RAW_TOPIC")
        .getOrElse(cfg.getString("kafka.raw-topic")),
      groupId = sys.env.get("NEWSPIPE_GROUP_ID")
        .getOrElse(cfg.getString("kafka.group-id")),
      bronzePath = sys.env.get("NEWSPIPE_BRONZE_PATH")
        .getOrElse(cfg.getString("bronze.path")),
      sparkMaster = sys.env.get("NEWSPIPE_SPARK_MASTER")
        .getOrElse(cfg.getString("spark.master")),
      opencodeModel = sys.env.get("NEWSPIPE_OPENCODE_MODEL")
        .getOrElse(cfg.getString("opencode.model")),
      opencodeTimeoutSeconds = sys.env.get("NEWSPIPE_OPENCODE_TIMEOUT_SECONDS").map(_.toLong)
        .getOrElse(cfg.getLong("opencode.timeout-seconds"))
    )
