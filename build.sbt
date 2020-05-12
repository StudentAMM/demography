name := "demography"

version := "0.1"

scalaVersion := "2.12.7"

val circeVersion = "0.12.0-M2"

val circe = Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.2",
  "org.apache.spark" %% "spark-hive" % "2.4.2",
  "org.apache.spark" %% "spark-mllib" % "2.4.2",
  "org.apache.spark" %% "spark-streaming" % "2.4.2",
  "org.postgresql" % "postgresql" % "42.1.1"
) ++ circe