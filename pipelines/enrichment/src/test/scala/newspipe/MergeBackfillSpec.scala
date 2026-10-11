package newspipe

import newspipe.application.ports.*
import newspipe.application.services.EnrichmentService
import newspipe.domain.models.*
import newspipe.domain.valueObjects.{GeoArea, Urgency}
import newspipe.infrastructure.bronze.DeltaMergeSink
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.*
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files
import java.time.Instant

class MergeBackfillSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll:
  private var spark: SparkSession = _
  override def beforeAll(): Unit =
    spark = SparkSession.builder()
      .appName("newspipe-merge-test")
      .master("local[2]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  override def afterAll(): Unit = spark.stop()

  private def item(g: String) = RssItem(g, "https://feed/x", s"T$g", s"https://a/$g", None, "Body")

  describe("EnrichmentService.runOnce") {
    it("merges successes and skips failures without committing them away") {
      var committed = false
      var merged: List[BronzeUnit] = Nil
      val consumer = new RawConsumer:
        def poll(): List[RssItem] = List(item("g1"), item("g2"))
        def commit(): Unit = committed = true
      val port = new EnrichmentPort:
        def enrich(i: RssItem): Either[Throwable, Enrichment] =
          if i.guid == "g2" then Left(new RuntimeException("boom"))
          else Right(Enrichment("S", Nil, GeoArea("Global"), Nil, Nil, Urgency("low"), "en"))
      val sink = new BronzeMergeSink:
        def merge(units: List[BronzeUnit]): Int =
          merged = units
          units.size
      val now = Instant.parse("2026-10-10T10:00:00Z")
      new EnrichmentService(consumer, port, sink).runOnce(now) shouldBe 1
      merged.map(_.id) shouldBe List(BronzeUnit.idFor(item("g1")))
      committed shouldBe true
    }
  }

  describe("DeltaMergeSink.merge") {
    it("updates the enriched row without duplicating") {
      val sp = spark
      import sp.implicits.*
      val now = Instant.parse("2026-10-10T10:00:00Z")
      val raw = BronzeUnit.fromRss(item("g1"), Enrichment.empty, now)
      val schema = StructType(Array(
        StructField("id", StringType), StructField("feed_url", StringType),
        StructField("article_url", StringType), StructField("title", StringType),
        StructField("content", StringType), StructField("published_at", StringType),
        StructField("ingested_at", StringType), StructField("summary", StringType),
        StructField("countries", ArrayType(StringType)), StructField("geo_area", StringType),
        StructField("topics", ArrayType(StringType)), StructField("actors", ArrayType(StringType)),
        StructField("urgency", StringType), StructField("language", StringType),
        StructField("ingested_date", StringType)))
      val row = Row(raw.id, raw.feedUrl, raw.articleUrl, raw.title, raw.content,
        raw.publishedAt.map(_.toString).orNull, raw.ingestedAt.toString, raw.summary,
        raw.countries.toArray, raw.geoArea.value, raw.topics.toArray, raw.actors.toArray,
        raw.urgency.value, raw.language,
        raw.ingestedAt.atZone(java.time.ZoneOffset.UTC).toLocalDate.toString)
      val setup = spark.createDataFrame(spark.sparkContext.parallelize(Seq(row)), schema)
      val path = Files.createTempDirectory("merge-test").toString
      setup.write.format("delta").mode("overwrite").save(path)
      val en = Enrichment("Sum.", List("Iran"), GeoArea("Middle East"), List("energy"), Nil, Urgency("high"), "en")
      new DeltaMergeSink(spark, path).merge(List(BronzeUnit.fromRss(item("g1"), en, now))) shouldBe 1
      val df = spark.read.format("delta").load(path)
      df.count() shouldBe 1
      df.select("summary").as[String].collect().toList shouldBe List("Sum.")
    }
    it("merges nothing on empty input without touching Spark") {
      new DeltaMergeSink(spark, "/tmp/merge-test-no-touch").merge(Nil) shouldBe 0
    }
  }
