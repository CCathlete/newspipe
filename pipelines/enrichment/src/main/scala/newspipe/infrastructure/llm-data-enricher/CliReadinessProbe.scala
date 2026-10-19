package newspipe.infrastructure.llmdataenricher

import newspipe.application.ports.{CliReadinessProbe, ProbeResult}

import java.nio.file.{Files, Path, Paths}
import scala.util.Try

final class OpenCodeCliReadinessProbe(binary: String) extends CliReadinessProbe:
  def name: String = "opencode-cli"

  def check(): ProbeResult =
    if binary.isEmpty then ProbeResult.NotReady(name, "no binary name configured")
    else
      val pathVar: Option[String] = sys.env.get("PATH")
      pathVar match
        case None => ProbeResult.NotReady(name, "PATH is not set")
        case Some(p) =>
          val candidates: List[Path] = p.split(java.io.File.pathSeparator).toList.map(Paths.get(_)).filter(d => Files.isDirectory(d)).map(d => d.resolve(binary))
          candidates.find(Files.isExecutable) match
            case Some(found) => ProbeResult.Ready
            case None => ProbeResult.NotReady(name, s"'$binary' is not executable on PATH")
