import Settings._
import Testing._

Revolver.settings

Revolver.enableDebugging(port = 5050, suspend = false)

// When JAVA_OPTS are specified in the environment, they are usually meant for the application
// itself rather than sbt, but they are not passed by default to the application, which is a forked
// process. This passes them through to the "re-start" command, which is probably what a developer
// would normally expect.
javaOptions in Revolver.reStart := sys.env("JAVA_OPTS").split(" ")

net.virtualvoid.sbt.graph.Plugin.graphSettings

lazy val rawlsModel: Project = (project in file("model"))
  .settings(modelSettings:_*)
  .withTestSettings

lazy val rawls = project.in(file("core"))
  .settings(rawlsSettings:_*)
  .dependsOn(rawlsModel)
  .withTestSettings
