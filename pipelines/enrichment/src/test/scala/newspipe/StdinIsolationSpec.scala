package newspipe

import newspipe.application.ports.ProbeResult
import newspipe.domain.models.RssItem
import newspipe.infrastructure.llmdataenricher.{OpenCodeCliClient, OpenCodeModelReadinessProbe}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path}
import scala.concurrent.duration.*

class StdinIsolationSpec extends AnyFunSpec with Matchers:
  private val item: RssItem = RssItem("g1", "https://feed/x", "T", "https://a/g1", None, "Body")

  private def fakeOpenCode(answer: String): Path =
    val script: Path = Files.createTempFile("fake-opencode", ".sh")
    Files.writeString(
      script,
      s"""#!/bin/sh
         |cat >/dev/null
         |printf '%s\\n' '$answer'
         |""".stripMargin
    )
    script.toFile.setExecutable(true)
    script

  private val enrichmentAnswer: String =
    "{\"type\":\"text\",\"part\":{\"text\":\"{\\\"summary\\\":\\\"S\\\",\\\"countries\\\":[],\\\"geoArea\\\":\\\"Global\\\",\\\"topics\\\":[],\\\"actors\\\":[],\\\"urgency\\\":\\\"low\\\",\\\"language\\\":\\\"en\\\"}\"}}"

  describe("stdin isolation") {
    it("enriches when the child would otherwise block on the inherited pipe") {
      val binary: Path = fakeOpenCode(enrichmentAnswer)
      val result = new OpenCodeCliClient("fake/model", 5.seconds, binary.toString).enrich(item)
      result.isRight shouldBe true
      result.toOption.map(_.summary) shouldBe Some("S")
    }

    it("reports ready when the child would otherwise block on the inherited pipe") {
      val binary: Path = fakeOpenCode("{\"type\":\"text\",\"part\":{\"text\":\"OK\"}}")
      new OpenCodeModelReadinessProbe("fake/model", 5.seconds, binary.toString).check() shouldBe ProbeResult.Ready
    }
  }