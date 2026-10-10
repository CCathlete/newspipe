ThisBuild / organization := "newspipe"
ThisBuild / version := "0.2.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.3.4"

// Spark 3.5.1 / Delta 3.1.0 ship Scala 2.13 artifacts (used from Scala 3 via
// CrossVersion.for3Use2_13) and pull scala-xml_2.13. ScalaTest pulls
// scala-xml_3, which would clash on cross-version suffix; exclude it so the
// binary-compatible scala-xml_2.13 satisfies both.
ThisBuild / excludeDependencies += ExclusionRule("org.scala-lang.modules", "scala-xml_3")

lazy val root = (project in file("."))
  .settings(
    name := "newspipe",
    libraryDependencies ++= Seq(
      "com.rometools" % "rome" % "2.1.0",
      "io.circe" %% "circe-core" % "0.14.10",
      "io.circe" %% "circe-generic" % "0.14.10",
      "io.circe" %% "circe-parser" % "0.14.10",
      "org.typelevel" %% "cats-effect" % "3.5.4",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
      "ch.qos.logback" % "logback-classic" % "1.5.8",
      "com.typesafe" % "config" % "1.4.3",
      "org.apache.kafka" % "kafka-clients" % "3.7.0",
      ("org.apache.spark" %% "spark-sql" % "3.5.1" % Provided).cross(CrossVersion.for3Use2_13),
      ("io.delta" %% "delta-spark" % "3.1.0").cross(CrossVersion.for3Use2_13),
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
      "org.typelevel" %% "cats-effect-testing-scalatest" % "1.5.0" % Test
    ),
    run / fork := true,
    Test / fork := true,
    // spark-sql stays Provided (cu-001 frame), but Provided jars sit on
    // Compile/fullClasspath, not Runtime/fullClasspath — the default `sbt run`
    // therefore dies with NoClassDefFoundError SparkSession. Run from
    // Compile/fullClasspath instead so feat-1 criterion 1 (`sbt run` boots)
    // holds without unpicking the Provided scope.
    Compile / run := Defaults.runTask(
      Compile / fullClasspath,
      Compile / run / mainClass,
      Compile / run / runner
    ).evaluated,
    run / javaOptions ++= Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.regex=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    ),
    Test / javaOptions ++= (run / javaOptions).value
  )
