name := "CDRReplay"

version := "0.1"

scalaVersion := "2.12.2"

enablePlugins(JavaAppPackaging)

libraryDependencies ++=
  Seq(
    "org.apache.kafka" %% "kafka" % "2.5.0"
  )
