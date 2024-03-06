import Settings.*

lazy val rawlsTests = project
  .in(file("."))
  .settings(rootSettings: _*)

version := "1.0"
