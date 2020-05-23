name := "CDRAnalyzer"

version := "0.1"

scalaVersion := "2.12.11"

enablePlugins(JavaAppPackaging)

libraryDependencies ++=
  Seq(
    "org.apache.spark" %% "spark-sql" % "2.4.5",
    "org.apache.spark" %% "spark-streaming" 	 % "2.4.5" % "provided",
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.5",
    "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.5"
  )
