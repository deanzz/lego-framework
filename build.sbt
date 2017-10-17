name := "lego-framework"

version := "3.0.0"

scalaVersion := "2.11.8"

lazy val legoCommon = project.in(file("modules/lego-common"))

lazy val legoGraph = project.in(file("modules/lego-graph")).aggregate(legoCommon)
  .dependsOn(legoCommon)

lazy val legoCore = project.in(file("modules/lego-core")).aggregate(legoCommon, legoGraph)
  .dependsOn(legoCommon, legoGraph)

lazy val root = project.in(file("."))

//assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)