name          := "Rawls"

organization  := "org.broadinstitute"

version       := "0.1"

scalaVersion  := "2.11.2"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

libraryDependencies ++= {
  val akkaV = "2.3.6"
  val sprayV = "1.3.2"
  Seq(
    //"org.json4s"             %% "json4s-native"  % "3.2.11",
    //"com.github.simplyscala" %% "scalatest-embedmongo" % "0.2.2" % "test",
    "com.typesafe.akka" %%  "akka-actor"    % akkaV,
    "com.typesafe.akka" %%  "akka-testkit"  % akkaV   % "test",
    "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
    "io.spray" %% "spray-can" % sprayV,
    "io.spray" %% "spray-routing" % sprayV,
    "io.spray" %% "spray-client" % sprayV,
    "io.spray" %% "spray-http" % sprayV,
    "io.spray" %% "spray-json" % "1.3.1",
    "io.spray" %% "spray-testkit" % sprayV  % "test",
    "org.scalatest" %% "scalatest" % "2.2.4" % "test",
    "org.mongodb" %% "casbah" % "2.8.0"
  )
}
