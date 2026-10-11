package newspipe.infrastructure.kafka

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder
import io.circe.parser.*
import newspipe.application.ports.RawConsumer
import newspipe.domain.models.RssItem
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.{Duration, Instant}
import java.util.Properties
import scala.jdk.CollectionConverters.*

object KafkaConsumerCodecs:
  given Decoder[Instant] = Decoder.decodeString.map(Instant.parse)
  given Decoder[RssItem] = deriveDecoder[RssItem]

final class KafkaRawConsumer(bootstrapServers: String, topic: String, groupId: String) extends RawConsumer with AutoCloseable:
  private val consumer = new KafkaConsumer[String, String](KafkaRawConsumer.props(bootstrapServers, groupId))
  consumer.subscribe(java.util.List.of(topic))

  def poll(): List[RssItem] =
    import KafkaConsumerCodecs.given
    consumer.poll(Duration.ofSeconds(10)).asScala.toList.flatMap { rec =>
      parse(rec.value()).toOption.flatMap(_.as[RssItem].toOption)
    }

  def commit(): Unit = consumer.commitSync()

  def close(): Unit = consumer.close()

object KafkaRawConsumer:
  def props(bootstrapServers: String, groupId: String): Properties =
    val p = new Properties()
    p.put("bootstrap.servers", bootstrapServers)
    p.put("group.id", groupId)
    p.put("key.deserializer", classOf[StringDeserializer].getName)
    p.put("value.deserializer", classOf[StringDeserializer].getName)
    p.put("enable.auto.commit", "false")
    p.put("auto.offset.reset", "earliest")
    p.put("max.poll.interval.ms", "3600000")
    p.put("max.poll.records", "20")
    p
