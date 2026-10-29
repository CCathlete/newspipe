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

  private def fakeOpenCode(answer: String, failOn: String): Path =
    val script: Path = Files.createTempFile("fake-opencode", ".sh")
    Files.writeString(
      script,
      s"""#!/bin/sh
         |cat >/dev/null
         |case "$$*" in
         |  *"$failOn"*) printf '%s\\n' '{"type":"error"}'; exit 1 ;;
         |  *) printf '%s\\n' '$answer'; exit 0 ;;
         |esac
         |""".stripMargin
    )
    script.toFile.setExecutable(true)
    script

  private val enrichmentAnswer: String =
    "{\"type\":\"text\",\"part\":{\"text\":\"{\\\"summary\\\":\\\"S\\\",\\\"countries\\\":[],\\\"geoArea\\\":\\\"Global\\\",\\\"topics\\\":[],\\\"actors\\\":[],\\\"urgency\\\":\\\"low\\\",\\\"language\\\":\\\"en\\\"}\"}}"

  describe("stdin isolation") {
    it("enriches when the child would otherwise block on the inherited pipe") {
      val binary: Path = fakeOpenCode(enrichmentAnswer, "never")
      val result = new OpenCodeCliClient(5.seconds, binary.toString).enrich(item, "fake/model")
      result.isRight shouldBe true
      result.toOption.map(_.summary) shouldBe Some("S")
    }

    it("reports ready when the child would otherwise block on the inherited pipe") {
      val binary: Path = fakeOpenCode("{\"type\":\"text\",\"part\":{\"text\":\"OK\"}}", "never")
      new OpenCodeModelReadinessProbe(List("fake/model"), 5.seconds, binary.toString).check() shouldBe ProbeResult.Ready
    }

    it("reports ready when one model in the chain answers and another does not") {
      val binary: Path = fakeOpenCode("{\"type\":\"text\",\"part\":{\"text\":\"OK\"}}", "bad/model")
      new OpenCodeModelReadinessProbe(List("bad/model", "good/model"), 5.seconds, binary.toString).check() shouldBe ProbeResult.Ready
    }

    it("names every configured model when none answers") {
      val binary: Path = fakeOpenCode("{\"type\":\"text\",\"part\":{\"text\":\"OK\"}}", "bad/model")
      val result = new OpenCodeModelReadinessProbe(List("bad/model"), 5.seconds, binary.toString).check()
      result shouldBe a[ProbeResult.NotReady]
      result.asInstanceOf[ProbeResult.NotReady].detail should include("bad/model")
    }
  }