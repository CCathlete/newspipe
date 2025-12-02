package notebookCode

// 1. Imports
import scala.io.Source
import org.apache.spark.sql.SparkSession
import java.util.Properties

object TrinoExploration {
// 2. Start Spark session
  val spark = SparkSession
    .builder()
    .appName("TrinoNotebook")
    .master("local[*]") // for notebooks; adjust for clusters
    .getOrCreate()

// 3. Load .env file
  val env = Source
    .fromFile(".env")
    .getLines()
    .filter(line => line.nonEmpty && !line.startsWith("#"))
    .map { line =>
      val Array(key, value) = line.split("=", 2)
      key -> value
    }
    .toMap

  val trinoUrl = env(
    "TRINO_URL"
  ) // e.g., "jdbc:trino://localhost:8080/hive/default"
  val trinoUser = env("TRINO_USER") // e.g., "user1"
  val trinoPassword = env.getOrElse("TRINO_PASSWORD", "")

// 4. JDBC properties
  val trinoProperties = new Properties()
  trinoProperties.setProperty("user", trinoUser)
  if (trinoPassword.nonEmpty)
    trinoProperties.setProperty("password", trinoPassword)

// 5. Function to run SQL on Trino and get a DataFrame
  def trinoQuery(sql: String) = {
    spark.read
      .jdbc(trinoUrl, s"($sql) AS subquery", trinoProperties)
  }

// 6. Example query
  val df = trinoQuery("SELECT * FROM my_silver_table LIMIT 20")

// 8. Optional: print schema
  df.printSchema()
}
