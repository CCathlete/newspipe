package newspipe.infrastructure.source


import io.circe.parser.decode

import io.circe.syntax.*

import newspipe.application.ports.ValidationResultStore

import newspipe.domain.models.SourceValidationModels.given

import newspipe.domain.models.ValidationSnapshot


import java.nio.file.Files

import java.nio.file.Path

import java.nio.file.StandardCopyOption

import scala.util.Try


final class JsonValidationResultStore(path: String) extends ValidationResultStore:

  def load(): Either[String, Option[ValidationSnapshot]] =

    val file: Path = Path.of(path)

    if !Files.isRegularFile(file) then Right(None)

    else

      decode[ValidationSnapshot](Files.readString(file)) match

        case Right(snapshot) => Right(Some(snapshot))

        case Left(error) => Left("validation results " + path + ": " + error.getMessage)



  def save(snapshot: ValidationSnapshot): Either[String, Unit] =

    Try {

      val file: Path = Path.of(path)

      val parent: Path = file.getParent

      if parent != null then Files.createDirectories(parent)

      val temporary: Path = Path.of(path + ".tmp")

      Files.writeString(temporary, snapshot.asJson.noSpaces)

      Files.move(temporary, file, StandardCopyOption.REPLACE_EXISTING)

      ()

    }.toEither.left.map(error => "save validation results " + path + ": " + error.getMessage)