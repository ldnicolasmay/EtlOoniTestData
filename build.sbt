name := "EtlOoniTestData"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.5",
  "org.apache.spark" %% "spark-sql" % "2.4.5",
  "org.apache.hadoop" % "hadoop-aws" % "2.7.3",
  "com.amazonaws" % "aws-java-sdk" % "1.7.4",
  "com.typesafe" % "config" % "1.3.0",
  "com.lihaoyi" %% "os-lib" % "0.2.9"
)
