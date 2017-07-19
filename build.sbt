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
  .dependsOn(rawlsModel)
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


// This appears to do some magic to configure itself. It consistently fails in some environments
// unless it is loaded after the settings definitions above.
Revolver.settings

mainClass in Revolver.reStart := Some("org.broadinstitute.dsde.rawls.Boot")

Revolver.enableDebugging(port = 5050, suspend = false)

// When JAVA_OPTS are specified in the environment, they are usually meant for the application
// itself rather than sbt, but they are not passed by default to the application, which is a forked
// process. This passes them through to the "re-start" command, which is probably what a developer
// would normally expect.
javaOptions in Revolver.reStart ++= sys.env("JAVA_OPTS").split(" ").toSeq
