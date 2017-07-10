name := "lego-common"

version := "2.0.5"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.1" % "provided",
  "joda-time" % "joda-time" % "2.9",
  "org.joda" % "joda-convert" % "1.8.1",
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "com.typesafe" % "config" % "1.2.1",
  "javax.mail" % "javax.mail-api" % "1.5.5",
  //"com.ning" % "async-http-client" % "1.9.38",
  "org.scalaj" %% "scalaj-http" % "2.3.0"
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)