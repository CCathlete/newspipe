import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.slf4j.LoggerFactory
import org.slf4j.Logger

object MarketingAttributionJob2 {
  val logger: Logger = LoggerFactory.getLogger(getClass().getName())

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
    var orderBy: String = "click_time_sec desc"
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

    orderBy = "view_time_sec asc"
    val firstCampaignsSales: Option[DataFrame] =
      for (sDF <- salesClicks) yield {
        val result: DataFrame = sesh.sql(s"""
          select * from (
          select *,
          row_number() over (partition by $partKeys
          order by $orderBy
          ) as rn
          from sales_campaigns
          )
          where rn = 1
          """)

        result.createOrReplaceTempView(("firstcampaigns_sales"))
        result
      }

    val finalAttributeJoined: Option[DataFrame] =
      for (lcsDF <- lastClicksSales; fcs <- firstCampaignsSales) yield {
        val result: DataFrame = sesh.sql(s"""
          select
            lcs.sale_id,
            cast(lcs.revenue as int) as revenue,
            from_unixtime(lcs.sale_time_sec) as sale_timestamp,
            year(from_unixtime(lcs.sale_time_sec)) as sale_year,
            month(from_unixtime(lcs.sale_time_sec)) as sale_month,
            dayofmonth(from_unixtime(lcs.sale_time_sec)) as sale_day,
            lcs.user_id,
            click_id,
            from_unixtime(click_time_sec) as last_click_time,
            lcs.campaign_id as last_click_campaign_id,
            fcs.campaign_id as first_campaign_id,
            from_unixtime(view_time_sec) as first_campaign_view_time,
            current_timestamp() as processedat
          from
            lastclicks_sales lcs
          join
            firstcampaigns_sales fcs using(sale_id)
          """)

        result.write
          .mode("overwrite")
          .partitionBy("sale_year", "sale_month")
          .parquet(s"$GOLD_PATH/campaign-click-sale")

        result
      }

    logger.info("Successfully wrote data to the Gold layer.")

    val creationQuery: String = s"""
    create table if not exists $TARGET_REDSHIFT_TABLE (
    sale_id bigint not null,
    revenue integer,
    sale_timestamp timestamp without time zone,
    sale_year integer,
    sale month integer,
    sale_day integer,
    user_id text not null,
    click_id text not null,
    last_click_time timestamp without time zone,
    last_click_campaign_id text,
    first_campaign_id text,
    first_campaign_view_time timestamp without time zone,
    processedat timestamp without time zone
    )
    distkey(sale_id)
    sortkey(sale_timestamp)
    """
    var writeOpts: Map[String, String] = Map(
      "url" -> REDSHIFT_URI,
      "dbtable" -> "dummy_for_creation",
      "driver" -> "com.amazon.redshift.jdbc.Driver",
      "preactions" -> creationQuery,
      "mode" -> "overwrite"
    )

    sesh.emptyDataFrame.write
      .format("jdbc")
      .options(writeOpts)
      .save()

    finalAttributeJoined.map(df => {
      df.write
        .format("io.github.spark-redshift-connector")
        .options(
          Map(
            "url" -> REDSHIFT_URI,
            "dbtable" -> TARGET_REDSHIFT_TABLE,
            "tempdir" -> s"$GOLD_PATH/redshift_temp/",
            "aws_iam_role" -> "arn:aws:iam::xxxxxxxxxx:role/RedshiftS3AccessRole",
            "mode" -> "append"
          )
        )
        .save()
    })

    logger.info(
      s"Successfully wrote data to redshift table: $TARGET_REDSHIFT_TABLE"
    )

    sesh.stop()

  }
}
