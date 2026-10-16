package newspipe

import newspipe.infrastructure.llmdataenricher.OpenCodeCliClient
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class OpenCodeTimeoutSpec extends AnyFunSpec with Matchers:
  describe("OpenCodeCliClient.killTree") {
    it("destroys the wrapper and all descendants") {
      val proc = new ProcessBuilder("sh", "-c", "sleep 120").start()
      val deadline = System.currentTimeMillis() + 5000L
      while proc.descendants().count() == 0L && System.currentTimeMillis() < deadline do Thread.sleep(50)
      proc.descendants().count() should be >= 1L
      OpenCodeCliClient.killTree(proc)
      proc.isAlive shouldBe false
      proc.descendants().count() shouldBe 0L
    }

    it("reaps the tree within a bounded time") {
      val proc = new ProcessBuilder("sh", "-c", "sleep 120").start()
      val startNanos = System.nanoTime()
      OpenCodeCliClient.killTree(proc)
      val elapsedMillis = (System.nanoTime() - startNanos) / 1000000L
      elapsedMillis should be < 15000L
    }
  }
