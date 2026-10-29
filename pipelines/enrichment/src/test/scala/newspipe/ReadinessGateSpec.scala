package newspipe

import newspipe.application.ports.{DependencyProbe, ProbeResult}
import newspipe.application.services.ReadinessGate
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import scala.concurrent.duration.*

class ReadinessGateSpec extends AnyFunSpec with Matchers:
  private class FixedProbe(n: String, r: ProbeResult) extends DependencyProbe:
    def name: String = n
    def check(): ProbeResult = r

  private val instant: Instant = Instant.parse("2026-10-17T00:00:00Z")

  describe("ReadinessGate") {
    it("reports ready when every probe passes") {
      val gate = new ReadinessGate(
        List(FixedProbe("a", ProbeResult.Ready), FixedProbe("b", ProbeResult.Ready)),
        5.seconds, 1.second, _ => (), () => instant)
      gate.await(instant).ready shouldBe true
      gate.await(instant).failures shouldBe empty
    }

    it("names every failing dependency") {
      var slept: Int = 0
      val clock: () => Instant = () => instant.plusSeconds(slept.toLong)
      val gate = new ReadinessGate(
        List(
          FixedProbe("kafka", ProbeResult.NotReady("kafka", "cannot reach localhost:9092")),
          FixedProbe("object-store", ProbeResult.NotReady("object-store", "bucket missing"))
        ),
        5.seconds, 1.second, _ => slept += 1, clock)
      val report = gate.await(instant)
      report.ready shouldBe false
      report.failures.size shouldBe 2
      report.describe should include("kafka")
      report.describe should include("object-store")
    }

    it("returns not-ready rather than blocking past maxWait") {
      var slept: Int = 0
      val clock: () => Instant = () => instant.plusSeconds(slept.toLong)
      val gate = new ReadinessGate(
        List(FixedProbe("kafka", ProbeResult.NotReady("kafka", "down"))),
        2.seconds, 1.second, _ => slept += 1, clock)
      val report = gate.await(instant)
      report.ready shouldBe false
      slept should be <= 2
    }

    it("returns not-ready immediately when maxWait is zero") {
      val gate = new ReadinessGate(
        List(FixedProbe("kafka", ProbeResult.NotReady("kafka", "down"))),
        Duration.Zero, 1.second, _ => (), () => instant)
      gate.await(instant).ready shouldBe false
    }

    it("stops at maxWait of wall-clock time when a probe is slower than the interval") {
      var now: Instant = instant
      var checks: Int = 0
      var slept: Int = 0
      val slow = new DependencyProbe:
        def name: String = "slow"
        def check(): ProbeResult =
          checks += 1
          now = now.plusSeconds(30)
          ProbeResult.NotReady("slow", "down")
      val gate = new ReadinessGate(
        List(slow), 60.seconds, 2.seconds, _ => slept += 1, () => now)
      gate.await(instant).ready shouldBe false
      checks shouldBe 2
      slept shouldBe 1
    }
  }