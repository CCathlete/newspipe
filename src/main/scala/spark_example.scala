import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.slf4j.LoggerFactory
import org.slf4j.Logger

object MarketingAttributionJob2 {

  def main(args: Array[String]): Unit = {
    val sesh: SparkSession = SparkSession
      .builder()
      .appName("AttributionGold")
      .getOrCreate()

    import sesh.implicits._

    val LOOKBACK_WINDOW_SECONDS: Int = 30 * 24 * 60 * 60
    val SILVER_PATH: String = "s3://my-data-lakehouse/silver"
    val GOLD_PATH: String = "s3://my-data-lakehouse/gold"
    val TARGET_REDSHIFT_TABLE: String = "campaignsalesattribution"
    val REDSHIFT_URI: String = "jdbc:redshift://myuser:mypass@address"

    val sales: Option[DataFrame] = Option(
      sesh.read.parquet(s"$SILVER_PATH/sales")
    )
    val clicks: Option[DataFrame] = Option(
      sesh.read.parquet(s"$SILVER_PATH/clicks")
    )
    val campaigns: Option[DataFrame] = Option(
      sesh.read.parquet(s"$SILVER_PATH/campaigns")
    )

    // Converting timestamps to long int (seconds from epoch).
    // The $ inside a dataframe is imported from sesh.implicits._
    // The meaning of it is create a column object col("purchase_timestamp").
    val salesWithSec: Option[DataFrame] = sales.map(df => {
      val transformedDF: DataFrame =
        df.withColumn("sale_time_sec", $"purchase_timestamp".cast(LongType))

      transformedDF.createOrReplaceTempView("sales")
      transformedDF
    })

    val clicksWithSec: Option[DataFrame] = clicks.map(df => {
      val transformedDF: DataFrame =
        df.withColumn("click_time_sec", $"click_timestamp".cast(LongType))

      transformedDF.createOrReplaceTempView("clicks")
      transformedDF
    })

    val campaignsWithSec: Option[DataFrame] = campaigns.map(df => {
      val transformedDF: DataFrame =
        df.withColumn("click_time_sec", $"click_timestamp".cast(LongType))

      transformedDF.createOrReplaceTempView("campaigns")
      transformedDF
    })

    val salesClicks: Option[DataFrame] =
      for (sDF <- salesWithSec; cDF <- clicksWithSec) yield {
        val result: DataFrame = sesh.sql(s"""
      select *
      from sales left join clicks using(user_id)
      where click_time_sec
      between (sale_time_sec - $LOOKBACK_WINDOW_SECONDS)
      and sale_time_sec
      """)

        result.createOrReplaceTempView(("sales_clicks"))
        result
      }

    val salesCampaigns: Option[DataFrame] =
      for (sDF <- salesWithSec; cDF <- campaignsWithSec) yield {
        val result: DataFrame = sesh.sql(s"""
      select *
      from sales left join campaigns using(user_id)
      where campaign_time_sec
      between (sale_time_sec - $LOOKBACK_WINDOW_SECONDS)
      and sale_time_sec
      """)

        result.createOrReplaceTempView(("sales_campaigns"))
        result
      }

    val partitionKeys: Seq[String] = Seq("sale_id")
    val partKeys: String = partitionKeys.mkString(", ")
    val orderBy: String = "click_time_sec desc"
    val lastClicksSales: Option[DataFrame] =
      for (sDF <- salesClicks) yield {
        val result: DataFrame = sesh.sql(s"""
      select * from (
      select *,
      row_number() over (partition by $partKeys
      order by $orderBy
      ) as rn
      from sales_clicks
      )
      where rn = 1
      """)

        result.createOrReplaceTempView(("lastclicks_sales"))
        result
      }

  }
}
