name := "lego-graph"

version := "3.0.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.1.1" % "provided",
  "com.typesafe.akka" % "akka-stream_2.11" % "2.5.3",
  "org.scalatest" % "scalatest_2.11" % "3.0.4" % "test"
)
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)