package newspipe

import com.typesafe.config.ConfigFactory
import newspipe.infrastructure.conf.ConfLoader
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

class ConfLoaderSpec extends AnyFunSpec with Matchers:
  private def chain(depth: Int): Path =
    val root: Path = Files.createTempDirectory("conf-chain")
    (0 until depth).foldLeft(root)((acc, _) => Files.createDirectories(acc.resolve("nested")))
    root

  private def write(dir: Path, name: String, body: String): Unit =
    Files.createDirectories(dir)
    Files.writeString(dir.resolve(name), body, StandardCharsets.UTF_8)

  describe("ConfLoader.discover") {
    it("finds conf/ by walking upward from a nested working directory") {
      val root: Path = chain(3)
      write(root.resolve("conf"), "feeds.conf", "newspipe { feeds = [\"http://a\"] }")
      ConfLoader.discover(root.resolve("nested").resolve("nested")).map(_.getFileName.toString) shouldBe Some("conf")
    }
    it("returns None when no conf/ exists on the path") {
      ConfLoader.discover(Files.createTempDirectory("conf-none")) shouldBe None
    }
  }

  describe("ConfLoader.load") {
    it("reports a named failure when conf/ is absent") {
      val result = ConfLoader.load("ingestion.conf", Files.createTempDirectory("conf-none"))
      result.isLeft shouldBe true
      result.left.toOption.map(_.contains("no 'conf' directory")) shouldBe Some(true)
    }
    it("reports a named failure when the pipeline file is missing") {
      val root: Path = chain(1)
      write(root.resolve("conf"), "feeds.conf", "newspipe { feeds = [] }")
      val result = ConfLoader.load("ingestion.conf", root)
      result.isLeft shouldBe true
      result.left.toOption.map(_.contains("ingestion.conf")) shouldBe Some(true)
    }
    it("merges the shared file under the pipeline file") {
      val root: Path = chain(1)
      write(root.resolve("conf"), "feeds.conf", "newspipe { feeds = [\"http://shared\"], poll-interval-seconds = 1 }")
      write(root.resolve("conf"), "ingestion.conf", "newspipe { poll-interval-seconds = 300 }")
      val cfg = ConfLoader.load("ingestion.conf", root).toOption.get.config.getConfig("newspipe")
      cfg.getStringList("feeds").asScala.toList shouldBe List("http://shared")
      cfg.getLong("poll-interval-seconds") shouldBe 300L
    }
  }
