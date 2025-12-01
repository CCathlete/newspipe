// --- Project Definition ---
name := "NewsPipe"
version := "1.0"
scalaVersion := "2.13.18" // Use a specific, stable patch version

// --- Repositories (Only standard ones) ---
resolvers ++= Seq(
  Resolver.mavenLocal,
  Resolver.defaultLocal,
  "MavenCentral" at "https://repo1.maven.org/maven2"
)

// --- Dependencies ---
libraryDependencies ++= Seq(
  // 1. Apache Spark Core and SQL (Spark 3.4.3 / Scala 2.13)
  "org.apache.spark" %% "spark-core" % "3.4.3" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.4.3" % "provided",

  // 2. Hadoop AWS / S3A (MinIO Connectivity)
  "org.apache.hadoop" % "hadoop-aws" % "3.3.4" % "provided",

  // 3. AWS SDK (Needed by Hadoop-AWS)
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.593" % "provided",

  // 5. PostgreSQL driver (Still useful for simple JDBC operations like DDL)
  "org.postgresql" % "postgresql" % "42.7.3",

  // 6. SLF4J API (For cleaner logging)
  "org.slf4j" % "slf4j-api" % "1.7.36" % "provided"
)
