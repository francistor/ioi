name := "CDRAnalyzer"

version := "0.1"

scalaVersion := "2.12.11"

enablePlugins(JavaAppPackaging)

libraryDependencies ++=
  Seq(
    "org.apache.spark" % "spark-streaming_2.12" 	 % "2.4.5" % "provided"
  )