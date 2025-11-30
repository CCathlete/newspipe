import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

// --- Standard Logging Setup (Placeholder for full Logback/SLF4J) ---
// In a production Scala application, you'd use a dedicated logging library.
// For simplicity here, we'll use println and exception handling.
// Alternatively, you can use the built-in Spark logging.

object MarketingAttributionJob {

  def main(args: Array[String]): Unit = {
    // 1. Initialize Spark Session
    // We assume the required packages (Hadoop-AWS, JDBC drivers, Spark-Redshift)
    // are configured via sbt/pom.xml or spark-submit flags.
    val sesh: SparkSession = SparkSession
      .builder()
      .appName("MarketingAttributionGold")
      .getOrCreate()

    // Import Spark implicits for DataFrame operations (e.g., $"" column selection)
    import sesh.implicits._

    // --- 2. Configuration Variables (use val for immutability) ---
    val LOOKBACK_WINDOW_SECONDS: Int = 30 * 24 * 60 * 60 // 30 days.
    val SILVER_PATH: String = "s3://my-data-lakehouse/silver"
    val GOLD_PATH: String = "s3://my-data-lakehouse/gold"
    val TARGET_REDSHIFT_TABLE: String = "campaignsalesattribution"
    val REDSHIFT_URI: String = "jdbc:redshift://myuser:mypass@address"

    // --- 3. Data Loading ---
    var sales: DataFrame = null
    var clicks: DataFrame = null
    var campaigns: DataFrame = null

    try {
      sales = sesh.read.parquet(s"$SILVER_PATH/sales")
      clicks = sesh.read.parquet(s"$SILVER_PATH/clicks")
      campaigns = sesh.read.parquet(s"$SILVER_PATH/campaigns")
      println("Successfully loaded all dataframes.")

    } catch {
      case e: Exception =>
        // Using standard logging approach here
        println(
          s"ERROR: Couldn't load data into sparkframes.\nError: ${e.getMessage}"
        )
        return // Exit the job if data loading fails
    }

    // --- 4. Preprocessing and Temporary Views ---

    // Converting timestamps to long int (seconds from epoch).
    val salesWithSec = sales
      .withColumn("sale_time_sec", $"purchase_timestamp".cast(LongType))
    salesWithSec.createOrReplaceTempView("sales")

    val clicksWithSec = clicks
      .withColumn("click_time_sec", $"click_timestamp".cast(LongType))
    clicksWithSec.createOrReplaceTempView("clicks")

    val campaignsWithSec = campaigns
      .withColumn("view_time_sec", $"view_timestamp".cast(LongType))
    campaignsWithSec.createOrReplaceTempView("campaigns")

    // --- 5. Windowed Joins and Attribution Logic ---

    // Scala uses the multiline string literal """...""" similar to Python for SQL.
    val salesClicks: DataFrame = sesh.sql(s"""
      SELECT *
      FROM sales
      LEFT JOIN clicks USING(user_id)
      WHERE click_time_sec
      BETWEEN (sale_time_sec - $LOOKBACK_WINDOW_SECONDS) AND sale_time_sec
    """)
    salesClicks.createOrReplaceTempView("sales_clicks")

    val salesCampaigns: DataFrame = sesh.sql(s"""
      SELECT *
      FROM sales
      LEFT JOIN campaigns USING(user_id)
      WHERE view_time_sec
      BETWEEN (sale_time_sec - $LOOKBACK_WINDOW_SECONDS) AND sale_time_sec
    """)
    salesCampaigns.createOrReplaceTempView("sales_campaigns")

    // --- 6. Last Click Attribution (Last Click) ---
    import org.apache.spark.sql.expressions.Window

    // Define the Window specification programmatically (more robust than SQL)
    val partitionKeys: Seq[String] = Seq("sale_id")
    val lastClickWindow = Window
      .partitionBy(partitionKeys.map(col): _*)
      .orderBy($"click_time_sec".desc)

    val lastClicksSales: DataFrame = salesClicks
      .select(
        col("*"),
        row_number().over(lastClickWindow).as("rn")
      )
      .where($"rn" === 1)

    lastClicksSales.createOrReplaceTempView("lastclicks_sales")

    // --- 7. First Campaign Attribution (First Touch) ---
    val firstCampaignWindow = Window
      .partitionBy(partitionKeys.map(col): _*)
      .orderBy($"view_time_sec".asc)

    val firstCampaignsSales: DataFrame = salesCampaigns
      .select(
        col("*"),
        row_number().over(firstCampaignWindow).as("rn")
      )
      .where($"rn" === 1)

    firstCampaignsSales.createOrReplaceTempView("firstcampaigns_sales")

    // --- 8. Final Join and Transformation ---

    val finalAttributeJoined: DataFrame = sesh.sql("""
      SELECT
        lcs.sale_id,
        CAST(lcs.revenue AS Int) AS revenue, -- Explicit CAST to Int (like ::int)
        from_unixtime(lcs.sale_time_sec) AS sale_timestamp, -- from_unixtime converts seconds to timestamp
        year(from_unixtime(lcs.sale_time_sec)) AS sale_year,
        month(from_unixtime(lcs.sale_time_sec)) AS sale_month,
        dayofmonth(from_unixtime(lcs.sale_time_sec)) AS sale_day,
        lcs.user_id,
        click_id,
        from_unixtime(click_time_sec) AS last_click_time,
        lcs.campaign_id AS last_click_campaign_id,
        fcs.campaign_id AS first_campaign_id,
        from_unixtime(view_time_sec) AS first_campaign_view_time,
        current_timestamp() AS processedat -- Spark built-in function
      FROM
        lastclicks_sales lcs
      JOIN
        firstcampaigns_sales fcs USING(sale_id)
    """)

    // --- 9. Write to Data Lake (Parquet) ---
    finalAttributeJoined.write
      .mode("overwrite") // Added mode: overwrite or append
      .partitionBy("sale_year", "sale_month")
      .parquet(s"$GOLD_PATH/campaign-click-sale/")

    println("Successfully wrote data to GOLD Parquet in S3.")

    // --- 10. Write to Redshift (SQL Pre-action) ---

    val creationQuery: String = s"""
      CREATE TABLE IF NOT EXISTS $TARGET_REDSHIFT_TABLE (
        sale_id bigint not null,
        revenue integer,
        sale_timestamp timestamp without time zone,
        sale_year integer,
        sale_month integer,
        sale_day integer,
        user_id text not null,
        click_id text not null,
        last_click_time timestamp without time zone,
        last_click_campaign_id text,
        first_campaign_id text,
        first_campaign_view_time timestamp without time zone,
        processedat timestamp without time zone
      )
      DISTKEY(sale_id)
      SORTKEY(sale_timestamp)
      ;
    """

    // 1. Create/manage the target table (Redshift creation query via JDBC preactions)
    // Note: Dropping the dummy table is often unnecessary in a dedicated JDBC connection.
    sesh.emptyDataFrame.write
      .format("jdbc")
      .options(
        "url",
        REDSHIFT_URI,
        "dbtable",
        "dummy_redshift",
        "driver",
        "com.amazon.redshift.jdbc.Driver",
        "preactions",
        creationQuery,
        "mode",
        "overwrite" // Only used to trigger the preactions
      )
      .save()

    // 2. Write the final data using the optimized Redshift connector
    finalAttributeJoined.write
      .format("io.github.spark-redshift-connector")
      .options(
        "url",
        REDSHIFT_URI,
        "dbtable",
        TARGET_REDSHIFT_TABLE,
        "tempdir",
        s"$GOLD_PATH/redshift_temp/",
        // Replace with your actual IAM role
        "aws_iam_role",
        "arn:aws:iam::xxxxxxxxxx:role/RedshiftS3AccessRole",
        "mode",
        "append"
      )
      .save()

    println(
      s"Successfully wrote data to Redshift table: $TARGET_REDSHIFT_TABLE"
    )

    sesh.stop()
  }
}
