name := "lego-framework"

version := "2.0.4"

scalaVersion := "2.10.6"

lazy val legoCommon = project.in(file("modules/framework-common"))

lazy val legoCore = project.in(file("modules/framework-core")).aggregate(legoCommon)
  .dependsOn(legoCommon)

lazy val root = project.in(file("."))

//assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)