package newspipe.infrastructure.bronze

import newspipe.application.ports.BronzeMergeSink
import newspipe.domain.models.BronzeUnit
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.*

final class DeltaMergeSink(spark: SparkSession, bronzePath: String) extends BronzeMergeSink:

  def merge(units: List[BronzeUnit]): Int =
    if units.isEmpty then 0
    else
      val rows = units.map { u =>
        Row(u.id, u.summary, u.countries.toArray, u.geoArea.value, u.topics.toArray, u.actors.toArray, u.urgency.value, u.language)
      }
      val schema = StructType(Array(
        StructField("id", StringType, nullable = false),
        StructField("summary", StringType, nullable = false),
        StructField("countries", ArrayType(StringType, containsNull = false), nullable = false),
        StructField("geo_area", StringType, nullable = false),
        StructField("topics", ArrayType(StringType, containsNull = false), nullable = false),
        StructField("actors", ArrayType(StringType, containsNull = false), nullable = false),
        StructField("urgency", StringType, nullable = false),
        StructField("language", StringType, nullable = false)
      ))
      val updates = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      updates.createOrReplaceTempView("updates")
      spark.sql(s"""MERGE INTO delta.`$bronzePath` AS bronze USING updates ON bronze.id = updates.id WHEN MATCHED THEN UPDATE SET bronze.summary = updates.summary, bronze.countries = updates.countries, bronze.geo_area = updates.geo_area, bronze.topics = updates.topics, bronze.actors = updates.actors, bronze.urgency = updates.urgency, bronze.language = updates.language""").count()
      units.size
