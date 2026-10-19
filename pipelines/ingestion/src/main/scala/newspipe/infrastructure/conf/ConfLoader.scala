package newspipe.infrastructure.conf

import com.typesafe.config.{Config, ConfigFactory}

import java.nio.file.{Files, Path}
import scala.annotation.tailrec
import scala.util.Try

final case class LoadedConf(config: Config, directory: Path)

object ConfLoader:
  private val DirName = "conf"
  private val SharedFile = "feeds.conf"

  def discover(start: Path): Option[Path] =
    @tailrec def walk(dir: Path): Option[Path] =
      if dir == null then None
      else
        val candidate: Path = dir.resolve(DirName)
        if Files.isDirectory(candidate) then Some(candidate)
        else walk(dir.getParent)
    walk(start.toAbsolutePath.normalize())

  def load(pipelineFile: String, start: Path): Either[String, LoadedConf] =
    discover(start) match
      case None => Left(s"no '$DirName' directory found at or above $start")
      case Some(dir) =>
        val shared: Path = dir.resolve(SharedFile)
        val own: Path = dir.resolve(pipelineFile)
        if !Files.isRegularFile(own) then Left(s"missing $own")
        else
          val parsedShared: Try[Config] = Try(ConfigFactory.parseFile(shared.toFile))
          val merged: Config =
            parsedShared.fold(_ => ConfigFactory.empty(), c => ConfigFactory.parseFile(own.toFile).withFallback(c).resolve())
          Right(LoadedConf(merged, dir))
