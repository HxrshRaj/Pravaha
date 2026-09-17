ThisBuild / scalaVersion := "2.13.18"
ThisBuild / version      := "0.1.0"
ThisBuild / organization := "pravaha"

// Spark 4.x is cross-built for Scala 2.13 only and has first-class Java 17/21
// support (no --add-opens juggling needed, unlike Spark 3.5.x on newer JDKs).
val sparkVersion = "4.1.3"

lazy val root = (project in file("."))
  .settings(
    name := "pravaha-batch",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql"  % sparkVersion,
      "org.postgresql"    % "postgresql" % "42.7.13",
      "org.scalatest"    %% "scalatest"  % "3.2.19" % Test,
    ),
    // Run Spark embedded (local[*]) as a plain forked JVM process via `sbt run`.
    // This *is* "a local Spark instance in standalone mode" - no cluster, no
    // separate Spark distribution, no spark-submit required.
    Compile / run / fork := true,
    Test / fork := true,
    Compile / run / javaOptions ++= Seq(
      "-Xmx2g",
      // Belt-and-suspenders alongside spark.sql.session.timeZone=UTC in
      // EventRollupJob: keep the forked JVM's own default zone UTC too, so
      // nothing that reads java.util.TimeZone.getDefault() (some JDBC driver
      // code paths do) can reintroduce the same shift on a non-UTC host.
      "-Duser.timezone=UTC",
      // Harmless on Java 21 / Spark 4, kept for anyone who pins an older JDK.
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
    ),
    Test / javaOptions ++= Seq("-Xmx2g", "-Duser.timezone=UTC"),
    Test / parallelExecution := false,
  )
