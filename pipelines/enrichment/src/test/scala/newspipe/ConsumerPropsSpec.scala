package newspipe

import newspipe.infrastructure.kafka.KafkaRawConsumer
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ConsumerPropsSpec extends AnyFunSpec with Matchers:
  describe("KafkaRawConsumer.props") {
    it("disables auto commit and bounds batch cost within the poll interval") {
      val p = KafkaRawConsumer.props("localhost:9092", "test-group")
      p.getProperty("enable.auto.commit") shouldBe "false"
      p.getProperty("auto.offset.reset") shouldBe "earliest"
      p.getProperty("max.poll.interval.ms") shouldBe "3600000"
      p.getProperty("max.poll.records") shouldBe "20"
    }
  }
