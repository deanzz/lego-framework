name := "lego-framework"

version := "2.0.4"

scalaVersion := "2.11.8"

lazy val legoCommon = project.in(file("modules/framework-common"))

lazy val legoGraph = project.in(file("modules/framework-graph")).aggregate(legoCommon)
  .dependsOn(legoCommon)

lazy val legoCore = project.in(file("modules/framework-core")).aggregate(legoCommon, legoGraph)
  .dependsOn(legoCommon, legoGraph)

lazy val root = project.in(file("."))

//assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)