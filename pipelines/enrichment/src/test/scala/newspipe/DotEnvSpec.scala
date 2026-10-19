package newspipe

import newspipe.infrastructure.env.DotEnv
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.*

class DotEnvSpec extends AnyFunSpec with Matchers:
  private def chain(depth: Int): Path =
    val root: Path = Files.createTempDirectory("dotenv-chain")
    (0 until depth).foldLeft(root)((acc, _) => Files.createDirectories(acc.resolve("a")))
    root

  private def write(dir: Path, body: String): Path =
    val f: Path = dir.resolve(".env")
    Files.writeString(f, body, StandardCharsets.UTF_8)
    f

  describe("DotEnv.discover") {
    it("returns an empty map when no .env exists on the path to the root") {
      DotEnv.discover(Files.createTempDirectory("dotenv-none")).values shouldBe empty
      DotEnv.discover(Files.createTempDirectory("dotenv-none")).source shouldBe None
    }
    it("returns the first .env found walking upward") {
      val root: Path = chain(3)
      write(root.resolve("a").resolve("a"), "NEWSPIPE_BRONZE_BUCKET=from-top\n")
      write(root.resolve("a"), "NEWSPIPE_BRONZE_BUCKET=from-middle\n")
      val found: DotEnv = DotEnv.discover(root.resolve("a").resolve("a"))
      found.get("NEWSPIPE_BRONZE_BUCKET") shouldBe Some("from-top")
      found.source.map(_.getFileName.toString) shouldBe Some(".env")
    }
    it("prefers the nearest .env over one further up") {
      val root: Path = chain(2)
      write(root.resolve("a"), "K=nearer\n")
      write(root, "K=farther\n")
      DotEnv.discover(root.resolve("a")).get("K") shouldBe Some("nearer")
    }
    it("returns the .env in the starting directory without consulting a parent") {
      val root: Path = chain(2)
      write(root.resolve("a"), "K=at-start\n")
      write(root, "K=one-level-up\n")
      DotEnv.discover(root.resolve("a")).get("K") shouldBe Some("at-start")
      DotEnv.discover(root.resolve("a")).source.map(_.getParent) shouldBe Some(root.resolve("a"))
    }
  }

  describe("DotEnv.expand") {
    it("expands a bare value") {
      DotEnv.expand(Map("A" -> "plain")).toOption.flatMap(_.get("A")) shouldBe Some("plain")
    }
    it("expands a reference") {
      DotEnv.expand(Map("A" -> "x", "B" -> "${A}y")).toOption.flatMap(_.get("B")) shouldBe Some("xy")
    }
    it("expands a nested reference") {
      DotEnv.expand(Map("A" -> "1", "B" -> "${A}", "C" -> "${B}${A}")).toOption.flatMap(_.get("C")) shouldBe Some("11")
    }
    it("treats a self reference as a hard error") {
      DotEnv.expand(Map("A" -> "${A}")).isLeft shouldBe true
    }
    it("treats a circular reference as a hard error") {
      DotEnv.expand(Map("A" -> "${B}", "B" -> "${A}")).isLeft shouldBe true
    }
    it("bounds expansion depth at ten") {
      val deep: Map[String, String] = (0 to 12).map(i => s"V$i" -> s"${if i == 12 then "end" else s"V${i + 1}"}").toMap
      DotEnv.expand(deep).isLeft shouldBe true
    }
  }

  describe("quoting") {
    it("keeps a single-quoted value literal") {
      DotEnv.expand(Map("A" -> "'${A}'")).toOption.flatMap(_.get("A")) shouldBe Some("${A}")
    }
    it("expands a double-quoted value") {
      DotEnv.expand(Map("A" -> "1", "B" -> "\"${A}\"")).toOption.flatMap(_.get("B")) shouldBe Some("${A}")
    }
    it("leaves an unquoted value alone when it has no reference") {
      DotEnv.expand(Map("A" -> "plain")).toOption.flatMap(_.get("A")) shouldBe Some("plain")
    }
  }
