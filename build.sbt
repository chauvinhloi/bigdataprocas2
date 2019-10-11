name := "bigdataprocas2"

version := "1.0.0"

scalaVersion := "2.11.8"

organization := "com.rmit"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0" % "provided"
)