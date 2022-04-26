import Settings._

lazy val rawlsTests = project.in(file("."))
  .settings(rootSettings:_*)

version := "1.0"

MinnieKenny.testSettings
testOptions in Test += Tests.Argument("-oD")
