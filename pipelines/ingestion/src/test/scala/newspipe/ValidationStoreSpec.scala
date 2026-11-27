package newspipe



import newspipe.domain.models.CandidateReport

import newspipe.domain.models.FeedValidation

import newspipe.domain.models.SourceCandidate

import newspipe.domain.models.ValidationSnapshot

import newspipe.domain.models.ValidationStatus

import newspipe.infrastructure.source.JsonValidationResultStore

import org.scalatest.funsuite.AnyFunSuite



import java.nio.file.Files



final class ValidationStoreSpec extends AnyFunSuite:

  test("snapshot round-trips through JSON") {

    val dir = Files.createTempDirectory("validation-store")

    val path: String = dir.resolve("nested").resolve("results.json").toString

    val validation: FeedValidation = FeedValidation("http://feed/rss", ValidationStatus.Usable,

      "ok", 1732610400000L, "feedType=rss_2.0", Some("rss_2.0"), 1, true, true)

    val snapshot: ValidationSnapshot = ValidationSnapshot(1, 1732610400000L,

      List(CandidateReport(SourceCandidate("pub", None, Some("curated"), List("http://feed/rss")), List(validation))))

    val store: JsonValidationResultStore = new JsonValidationResultStore(path)

    assert(store.save(snapshot).isRight)

    assert(store.load().toOption.get.contains(snapshot))

  }



  test("malformed snapshot returns a located diagnostic") {

    val dir = Files.createTempDirectory("validation-bad")

    val file = dir.resolve("results.json")

    Files.writeString(file, "{")

    val result = new JsonValidationResultStore(file.toString).load()

    assert(result.isLeft)

    assert(result.left.toOption.get.contains("results.json"))

  }
