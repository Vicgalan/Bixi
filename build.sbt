name := "bixi"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.2.0",
  "org.apache.spark" %% "spark-core" % "2.2.0",
  "org.scalatest" %% "scalatest" % "2.2.2" % Test
)