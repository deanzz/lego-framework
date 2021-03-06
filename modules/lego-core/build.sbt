name := "lego-core"

version := "3.0.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.1.1" % "provided",
  "org.scalatest" % "scalatest_2.11" % "3.0.4" % "test"/*,
  "com.typesafe" % "config" % "1.2.1",
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "joda-time" % "joda-time" % "2.9",
  "org.joda" % "joda-convert" % "1.8.1"*/
)
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)