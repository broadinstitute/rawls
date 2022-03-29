import Settings._
import Testing._

val compileAndTest = "compile->compile;test->test"

lazy val workbenchUtil = project.in(file("util"))
  .settings(utilSettings:_*)
  .withTestSettings

lazy val rawlsModel = project.in(file("model"))
  .settings(modelSettings:_*)
  .withTestSettings

lazy val workbenchMetrics = project.in(file("metrics"))
  .settings(metricsSettings:_*)
  .dependsOn(workbenchUtil % compileAndTest)
  .withTestSettings

lazy val workbenchGoogle = project.in(file("google"))
  .settings(googleSettings:_*)
  .dependsOn(rawlsModel)
  .dependsOn(workbenchUtil % compileAndTest)
  .dependsOn(workbenchMetrics % compileAndTest)
  .withTestSettings

lazy val rawlsCore = project.in(file("core"))
  .settings(rawlsCoreSettings:_*)
  .dependsOn(workbenchUtil % compileAndTest)
  .dependsOn(rawlsModel)
  .dependsOn(workbenchGoogle)
  .dependsOn(workbenchMetrics % compileAndTest)
  .withTestSettings

lazy val rawls = project.in(file("."))
  .settings(rootSettings:_*)
  .aggregate(workbenchUtil)
  .aggregate(workbenchGoogle)
  .aggregate(rawlsModel)
  .aggregate(workbenchMetrics)
  .aggregate(rawlsCore)
  .dependsOn(rawlsCore)
  .withTestSettings
