package newspipe.infrastructure.llmdataenricher

import newspipe.application.ports.{CliReadinessProbe, ProbeResult}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

/** Asks the configured model a trivial question at startup. The binary probe
  * only proves `opencode` is on PATH; this one proves the model answers, so a
  * model opencode no longer serves aborts the pipeline instead of silently
  * failing on every item. */
final class OpenCodeModelReadinessProbe(model: String, timeout: FiniteDuration, binary: String = "opencode") extends CliReadinessProbe:
  def name: String = "opencode-model"

  def check(): ProbeResult =
    if model.isEmpty then ProbeResult.NotReady(name, "no model configured")
    else
      Try {
        val proc = new ProcessBuilder(binary, "run", "--format", "json", "--model", model, "reply with exactly: OK")
          .redirectErrorStream(true)
          .redirectInput(ProcessBuilder.Redirect.from(new java.io.File("/dev/null")))
          .start()
        val finished = proc.waitFor(timeout.toMillis, TimeUnit.MILLISECONDS)
        if !finished then
          OpenCodeCliClient.killTree(proc)
          ProbeResult.NotReady(name, s"model '$model' did not answer within ${timeout.toSeconds} s")
        else
          val text = scala.io.Source.fromInputStream(proc.getInputStream).mkString
          if proc.exitValue() != 0 || text.contains("\"type\":\"error\"") then
            ProbeResult.NotReady(name, s"model '$model' did not answer: ${text.take(200)}")
          else ProbeResult.Ready
      }.getOrElse(ProbeResult.NotReady(name, s"could not invoke opencode for model '$model'"))
