// --- Project Definition ---
name := "NewsPipe"
version := "1.0"
scalaVersion := "2.13.18" // Use a specific, stable patch version

// --- Repositories (Only standard ones) ---
resolvers ++= Seq(
  Resolver.mavenLocal,
  Resolver.defaultLocal,
  "MavenCentral" at "https://repo1.maven.org/maven2",
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
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

  // 4. ZIO Toolkit (Functional Programming, ZIO, Effect Management)
  "dev.zio" %% "zio" % "2.1.23", // Stable ZIO 2.x
  "dev.zio" %% "zio-http" % "3.3.3", // For the OllamaClient REST call
  "io.getquill" %% "quill-zio" % "4.8.5", // Uses latest stable Quill with ZIO integration
  "io.getquill" %% "quill-jdbc" % "4.8.5",

  // 5. Type-Level Libraries (Circe for JSON handling with Ollama)
  "io.circe" %% "circe-core" % "0.14.6",
  "io.circe" %% "circe-generic" % "0.14.6",
  "io.circe" %% "circe-parser" % "0.14.6",

  // 6. Scala Scraping (OO/Encapsulation)
  "net.ruippeixotog" % "scala-scraper_2.13" % "3.2.0",

  // 7. PostgreSQL driver (Still useful for simple JDBC operations like DDL)
  "org.postgresql" % "postgresql" % "42.7.3",

  // 8. SLF4J API (For cleaner logging)
  "org.slf4j" % "slf4j-api" % "1.7.36" % "provided"
)
