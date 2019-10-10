name := "bigdataprocas2"

version := "0.4.4"

scalaVersion := "2.11.8"

organization := "com.rmit"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0" % "provided"
)