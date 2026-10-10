package newspipe.infrastructure

import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder
import io.circe.syntax.*
import newspipe.domain.{RawPublisher, RssItem}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.Properties

object KafkaCodecs:
  given Encoder[Instant] = Encoder.encodeString.contramap(DateTimeFormatter.ISO_INSTANT.format)
  given Encoder[RssItem] = deriveEncoder[RssItem]

final class KafkaRawPublisher(bootstrapServers: String, topic: String) extends RawPublisher with AutoCloseable:
  private val producer = new KafkaProducer[String, String](KafkaRawPublisher.props(bootstrapServers))

  def publish(items: List[RssItem]): Unit =
    import KafkaCodecs.given
    items.foreach { it =>
      producer.send(new ProducerRecord[String, String](topic, it.guid, it.asJson.noSpaces))
    }
    producer.flush()

  def close(): Unit = producer.close()

object KafkaRawPublisher:
  def props(bootstrapServers: String): Properties =
    val p = new Properties()
    p.put("bootstrap.servers", bootstrapServers)
    p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    p.put("acks", "all")
    p.put("enable.idempotence", "true")
    p
