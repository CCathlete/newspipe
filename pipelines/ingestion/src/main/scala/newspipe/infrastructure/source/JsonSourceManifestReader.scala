package newspipe.infrastructure.source

import io.circe.parser.parse

import newspipe.application.ports.SourceManifestReader

import newspipe.domain.models.SourceCandidate

import newspipe.domain.models.SourceValidationModels.given


import java.nio.file.Files

import java.nio.file.Path


final class JsonSourceManifestReader extends SourceManifestReader:

  def read(path: String): Either[String, List[SourceCandidate]] =

    val file: Path = Path.of(path)

    if !Files.isRegularFile(file) then Left("manifest not found: " + path)

    else

      val raw: String = Files.readString(file)

      parse(raw) match

        case Left(err) => Left("manifest " + path + ": " + err.getMessage)

        case Right(json) =>

          json.as[List[SourceCandidate]] match

            case Left(err) => Left("manifest " + path + ": " + err.getMessage)

            case Right(candidates) => Right(candidates)