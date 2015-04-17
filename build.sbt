name          := "Rawls"

organization  := "org.broadinstitute"

version       := "0.1"

scalaVersion  := "2.11.2"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

libraryDependencies ++= Seq(
  "org.scalatest"          %%  "scalatest"     % "2.2.4" % "test",
  "org.json4s"             %% "json4s-native"  % "3.2.11",
  "org.mongodb"            %% "casbah"         % "2.8.0"
  //"com.github.simplyscala" %% "scalatest-embedmongo" % "0.2.2" % "test"
)
