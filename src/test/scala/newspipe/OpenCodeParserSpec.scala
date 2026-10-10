package newspipe

import newspipe.infrastructure.OpenCodeCliClient
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class OpenCodeParserSpec extends AnyFunSpec with Matchers:
  private val events =
    """{"type":"step_start","part":{"type":"step-start"}}
       |{"type":"text","part":{"type":"text","text":"{\"summary\":\"Strait drills.\",\"countries\":[\"Iran\"],\"geoArea\":\"Middle East\",\"topics\":[\"energy\"],\"actors\":[],\"urgency\":\"high\",\"language\":\"en\"}"}}
       |{"type":"step_finish","part":{"type":"step-finish","reason":"stop"}}""".stripMargin
  describe("OpenCodeCliClient.parseEvents") {
    it("concats text parts and parses summary/countries/geoArea/tags") {
      val e = OpenCodeCliClient.parseEvents(events)
      e.summary shouldBe "Strait drills."
      e.countries shouldBe List("Iran")
      e.geoArea shouldBe "Middle East"
      e.topics shouldBe List("energy")
      e.urgency shouldBe "high"
    }
    it("defaults missing keys") {
      val e = OpenCodeCliClient.parseEnrichment("{\"summary\":\"x\"}")
      e.countries shouldBe Nil
      e.geoArea shouldBe "unknown"
      e.urgency shouldBe "low"
      e.language shouldBe "en"
    }
  }
