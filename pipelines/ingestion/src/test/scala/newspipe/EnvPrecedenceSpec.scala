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
    confDir.resolve("ingestion.conf"),
    """
      newspipe {
        feeds = ["http://conf/only"]
        poll-interval-seconds = 300
        kafka { bootstrap-servers = "localhost:9092", raw-topic = "raw_rss" }
        bronze { path = "s3a://lakehouse/bronze/news" }
        spark { master = "local[*]" }
        s3 {
          endpoint = "http://conf:9000"
          path-style-access = true
          access-key = "confkey"
          secret-key = "confsecret"
          bucket = "confbucket"
        }
      }
    """.stripMargin, StandardCharsets.UTF_8)
  private val cfg = ConfigFactory.parseFile(confDir.resolve("ingestion.conf").toFile).resolve().getConfig("newspipe")

  describe("ARCH.DOTENV.MERGE") {
    it("falls back to the conf file when no .env key is present") {
      val env = Env.load(cfg, DotEnv(Map.empty, None))
      env.s3Endpoint shouldBe "http://conf:9000"
      env.feeds shouldBe List("http://conf/only")
    }
    it("prefers a .env value over the conf file") {
      Env.load(cfg, DotEnv(Map("NEWSPIPE_S3_ENDPOINT" -> "http://dotenv:9000"), None)).s3Endpoint shouldBe "http://dotenv:9000"
    }
    it("reads the boolean and integer tiers through the same precedence") {
      val env = Env.load(cfg, DotEnv(Map("NEWSPIPE_S3_PATH_STYLE" -> "false", "NEWSPIPE_POLL_INTERVAL_SECONDS" -> "42"), None))
      env.s3PathStyle shouldBe false
      env.pollIntervalSeconds shouldBe 42
    }
    it("keeps an expanded .env value intact") {
      val raw = Map("NEWSPIPE_S3_ENDPOINT" -> "http://${NEWSPIPE_S3_HOST}:9000", "NEWSPIPE_S3_HOST" -> "rustfs")
      Env.load(cfg, DotEnv(DotEnv.expand(raw).toOption.get, None)).s3Endpoint shouldBe "http://rustfs:9000"
    }
    it("carries the object-store fields through to EnvConfig") {
      val env = Env.load(cfg, DotEnv.empty)
      env.s3Bucket shouldBe "confbucket"
      env.s3AccessKey shouldBe "confkey"
      env.s3SecretKey shouldBe "confsecret"
    }
  }
