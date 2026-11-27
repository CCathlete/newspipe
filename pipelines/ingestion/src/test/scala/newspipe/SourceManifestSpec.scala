package newspipe



import newspipe.domain.models.SourceCandidate

import newspipe.infrastructure.source.JsonSourceManifestReader

import org.scalatest.funsuite.AnyFunSuite



import java.nio.file.Files



final class SourceManifestSpec extends AnyFunSuite:

  private def candidateJson(index: Int): String =

    "{\"publisherId\":\"pub-" + index + "\",\"title\":\"Publisher " + index + "\",\"provenance\":\"curated-list\",\"urls\":[\"http://127.0.0.1:9911/rss\",\"http://127.0.0.1:9911/atom\"]}"



  test("manifest preserves identity, provenance and multiple URLs") {

    val dir = Files.createTempDirectory("sources-manifest")

    val file = dir.resolve("sources.json")

    Files.writeString(file, "[" + candidateJson(1) + ",{\"publisherId\":\"bad\",\"urls\":[\"not-a-url\"]}]")

    val loaded: Either[String, List[SourceCandidate]] = new JsonSourceManifestReader().read(file.toString)

    assert(loaded.isRight)

    val candidates: List[SourceCandidate] = loaded.toOption.get

    assert(candidates.size == 2)

    assert(candidates.head.publisherId == "pub-1")

    assert(candidates.head.provenance.contains("curated-list"))

    assert(candidates.head.urls == List("http://127.0.0.1:9911/rss", "http://127.0.0.1:9911/atom"))

    assert(candidates(1).urls == List("not-a-url"))

  }



  test("manifest rejects malformed JSON with a located diagnostic") {

    val dir = Files.createTempDirectory("sources-bad")

    val file = dir.resolve("bad.json")

    Files.writeString(file, "{")

    val result: Either[String, List[SourceCandidate]] = new JsonSourceManifestReader().read(file.toString)

    assert(result.isLeft)

    assert(result.left.toOption.get.contains("bad.json"))

  }



  test("manifest loads one thousand candidates") {

    val dir = Files.createTempDirectory("sources-large")

    val file = dir.resolve("large.json")

    val body: String = (1 to 1000).map(candidateJson).mkString("[", ",", "]")

    Files.writeString(file, body)

    val result: Either[String, List[SourceCandidate]] = new JsonSourceManifestReader().read(file.toString)

    assert(result.isRight)

    assert(result.toOption.get.size == 1000)

  }
