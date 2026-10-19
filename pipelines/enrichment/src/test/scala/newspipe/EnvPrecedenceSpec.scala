package newspipe

import com.typesafe.config.ConfigFactory
import newspipe.infrastructure.env.{DotEnv, Env}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

class EnvPrecedenceSpec extends AnyFunSpec with Matchers:
  private val confDir: Path = Files.createTempDirectory("env-conf").resolve("conf")
  Files.createDirectories(confDir)
  Files.writeString(
    confDir.resolve("enrichment.conf"),
    """
      newspipe {
        poll-interval-seconds = 300
        kafka { bootstrap-servers = "localhost:9092", raw-topic = "raw_rss", group-id = "enrichment-group" }
        bronze { path = "s3a://lakehouse/bronze/news" }
        spark { master = "local[*]" }
        opencode { model = "opencode/test", timeout-seconds = 120 }
        s3 {
          endpoint = "http://conf:9000"
          path-style-access = true
          access-key = "confkey"
          secret-key = "confsecret"
          bucket = "confbucket"
        }
      }
    """.stripMargin, StandardCharsets.UTF_8)
  private val cfg = ConfigFactory.parseFile(confDir.resolve("enrichment.conf").toFile).resolve().getConfig("newspipe")

  describe("ARCH.DOTENV.MERGE") {
    it("falls back to the conf file when no .env key is present") {
      val env = Env.load(cfg, DotEnv(Map.empty, None))
      env.s3Endpoint shouldBe "http://conf:9000"
      env.groupId shouldBe "enrichment-group"
      env.opencodeModel shouldBe "opencode/test"
      env.opencodeTimeoutSeconds shouldBe 120
    }
    it("prefers a .env value over the conf file") {
      Env.load(cfg, DotEnv(Map("NEWSPIPE_S3_ENDPOINT" -> "http://dotenv:9000"), None)).s3Endpoint shouldBe "http://dotenv:9000"
    }
    it("prefers a .env value for the enrichment-only keys") {
      val env = Env.load(cfg, DotEnv(Map("NEWSPIPE_OPENCODE_MODEL" -> "opencode/from-dotenv", "NEWSPIPE_KAFKA_GROUP_ID" -> "g2"), None))
      env.opencodeModel shouldBe "opencode/from-dotenv"
      env.groupId shouldBe "g2"
    }
    it("reads the boolean tier through the same precedence") {
      Env.load(cfg, DotEnv(Map("NEWSPIPE_S3_PATH_STYLE" -> "false"), None)).s3PathStyle shouldBe false
    }
    it("reads the long tier through the same precedence") {
      Env.load(cfg, DotEnv(Map("NEWSPIPE_POLL_INTERVAL_SECONDS" -> "60"), None)).pollIntervalSeconds shouldBe 60L
    }
  }
