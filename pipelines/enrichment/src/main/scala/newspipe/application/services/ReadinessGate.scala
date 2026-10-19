package newspipe.application.services

import newspipe.application.ports.{DependencyProbe, ProbeResult}

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

final case class ReadinessReport(ready: Boolean, failures: List[String]):
  def describe: String =
    if ready then "all dependencies ready"
    else failures.mkString("not ready: ", "; ", "")

final class ReadinessGate(
    probes: List[DependencyProbe],
    maxWait: FiniteDuration,
    interval: FiniteDuration,
    sleep: FiniteDuration => Unit
):
  def await(start: Instant): ReadinessReport =
    val deadline: Instant = start.plusMillis(maxWait.toMillis)
    var now: Instant = start
    var last: List[String] = Nil
    var continue: Boolean = true
    while continue do
      last = probes.flatMap: probe =>
        probe.check() match
          case ProbeResult.Ready => Nil
          case ProbeResult.NotReady(dep, detail) => List(s"$dep: $detail")
      if last.isEmpty then continue = false
      else if !now.isBefore(deadline) then continue = false
      else
        sleep(interval)
        now = now.plusMillis(interval.toMillis)
    ReadinessReport(last.isEmpty, last)
