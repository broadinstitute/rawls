import Settings._

lazy val rawlsTests = project.in(file("."))
  .settings(rootSettings:_*)

version := "1.0"

scalafixDependencies in ThisBuild += "org.scalatest" %% "autofix" % "3.1.0.0"

MinnieKenny.testSettings
