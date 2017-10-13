name := "lego-test-assembly"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.1.1" % "provided",
  "org.apache.spark" % "spark-sql_2.11" % "2.1.1" % "provided",
  "com.typesafe" % "config" % "1.3.1" % "provided"
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)