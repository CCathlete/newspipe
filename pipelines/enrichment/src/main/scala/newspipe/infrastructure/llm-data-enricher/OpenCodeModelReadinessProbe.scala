package newspipe.infrastructure.llmdataenricher

import newspipe.application.ports.{CliReadinessProbe, ProbeResult}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

final class OpenCodeModelReadinessProbe(
    models: List[String],
    timeout: FiniteDuration,
    binary: String = "opencode"
) extends CliReadinessProbe:
  def name: String = "opencode-model"

  def check(): ProbeResult =
    val configured: List[String] = models.map(_.trim).filter(_.nonEmpty).distinct
    if configured.isEmpty then ProbeResult.NotReady(name, "no model configured")
    else
      val failures: List[String] = configured.flatMap(model => checkOne(model))
      if failures.size < configured.size then ProbeResult.Ready
      else ProbeResult.NotReady(name, failures.mkString("; "))

  private def checkOne(model: String): Option[String] =
    Try {
      val proc = new ProcessBuilder(binary, "run", "--format", "json", "--model", model, "reply with exactly: OK")
        .redirectErrorStream(true)
        .redirectInput(ProcessBuilder.Redirect.from(new java.io.File("/dev/null")))
        .start()
      val finished = proc.waitFor(timeout.toMillis, TimeUnit.MILLISECONDS)
      if !finished then
        OpenCodeCliClient.killTree(proc)
        Some(s"model '$model' did not answer within ${timeout.toSeconds} s")
      else
        val source = scala.io.Source.fromInputStream(proc.getInputStream)
        val text = try source.mkString finally source.close()
        if proc.exitValue() != 0 || text.contains("\"type\":\"error\"") then
          Some(s"model '$model' did not answer: ${text.take(200)}")
        else None
    }.getOrElse(Some(s"could not invoke $binary for model '$model'"))