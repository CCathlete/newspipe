package newspipe

import newspipe.domain.{BronzeUnit, Enrichment, RssItem}
import newspipe.infrastructure.DeltaBronzeSink
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files
import java.time.Instant

class DeltaBronzeSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll:
  private var spark: SparkSession = _
  override def beforeAll(): Unit =
    spark = SparkSession.builder()
      .appName("newspipe-test")
      .master("local[2]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  override def afterAll(): Unit = spark.stop()
  describe("DeltaBronzeSink") {
    it("writes data + metadata as one unit per row") {
      val now = Instant.parse("2026-10-10T10:00:00Z")
      val item = RssItem("g1", "https://feed/x", "T", "https://a/1", None, "Body text")
      val en = Enrichment("Sum.", List("Iran"), "Middle East", List("energy"), Nil, "high", "en")
      val units = List(BronzeUnit.fromRss(item, en, now), BronzeUnit.fromRss(item.copy(guid = "g2"), en, now))
      val path = Files.createTempDirectory("bronze-test").toString
      new DeltaBronzeSink(spark, path).write(units) shouldBe 2
      val df = spark.read.format("delta").load(path)
      df.count() shouldBe 2
      df.columns should contain allOf ("title", "content", "article_url", "summary", "countries", "geo_area", "topics")
      val sp = spark
      import sp.implicits._
      df.select("summary").as[String].collect().toList.distinct shouldBe List("Sum.")
    }
  }
