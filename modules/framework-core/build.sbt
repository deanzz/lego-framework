name := "lego-core"

version := "2.0.4"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.1.1" % "provided"/*,
  "com.typesafe" % "config" % "1.2.1",
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "joda-time" % "joda-time" % "2.9",
  "org.joda" % "joda-convert" % "1.8.1"*/
)
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)