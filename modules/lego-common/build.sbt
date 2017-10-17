name := "lego-common"

version := "3.0.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.1.1" % "provided",
  "joda-time" % "joda-time" % "2.9",
  "org.joda" % "joda-convert" % "1.8.1",
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "com.typesafe" % "config" % "1.3.1",
  "javax.mail" % "javax.mail-api" % "1.5.5",
  //"com.ning" % "async-http-client" % "1.9.38",
  "org.scalaj" %% "scalaj-http" % "2.3.0",
  "org.scaldi" % "scaldi_2.11" % "0.5.8",
  "org.scaldi" % "scaldi-akka_2.11" % "0.5.8",
  "org.scalatest" % "scalatest_2.11" % "3.0.4" % "test"
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)