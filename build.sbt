// --- Project Definition ---
name := "NewsPipe" // Name of your project
version := "1.0" // Version of your project
scalaVersion := "2.13" // Ensure this matches the version Spark supports (2.12 is common)

// --- Repositories (Needed for some proprietary or less common libraries) ---
// This block ensures sbt can find Spark, AWS, and Redshift connector dependencies.
resolvers ++= Seq(
  Resolver.mavenLocal,
  Resolver.defaultLocal,
  Resolver.sonatypeRepo("public"),
  "SparkPackages" at "https://repo1.maven.org/maven2",
  "RedshiftRepo" at "https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.43.1067/RedshiftJDBC42.jar" // Example of specific repo if needed
)

// --- Dependencies ---
// The core dependencies for your Spark job
libraryDependencies ++= Seq(
  // 1. Apache Spark Core and SQL
  // 'provided' means the dependency is available in the target environment (Spark cluster)
  "org.apache.spark" %% "spark-core" % "3.4.3" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.4.3" % "provided",

  // 2. Hadoop AWS / S3A (MinIO Connectivity)
  // This is critical for connecting Spark to S3-compatible storage like MinIO
  "org.apache.hadoop" % "hadoop-aws" % "3.3.4" % "provided",

  // 3. AWS SDK (Needed by Hadoop-AWS for S3/MinIO operations)
  // The version must be compatible with the Hadoop version above (3.3.4)
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.593" % "provided",

  // 4. Spark Redshift Connector
  // For writing data efficiently to AWS Redshift (uses S3 staging)
  "io.github.spark-redshift-connector" % "spark-redshift" % "6.0.0" exclude (
    "org.apache.spark",
    "spark-sql_2.12"
  ),

  // 5. Redshift/PostgreSQL JDBC Driver
  // For establishing the JDBC connection (e.g., for creating the table with 'preactions')
  "org.postgresql" % "postgresql" % "42.7.3" // Redshift is based on PostgreSQL
)
