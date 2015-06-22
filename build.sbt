name          := "rawls"

organization  := "org.broadinstitute"

version       := "0.1"

scalaVersion  := "2.11.2"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

val artifactory = "https://artifactory.broadinstitute.org/artifactory/"

resolvers += "artifactory-releases" at artifactory + "libs-release"

resolvers += "artifactory-snapshots" at artifactory + "libs-snapshot"

libraryDependencies ++= {
  val akkaV = "2.3.6"
  val sprayV = "1.3.2"
  Seq(
    "com.gettyimages" %% "spray-swagger" % "0.5.0",
    "com.typesafe.akka" %% "akka-actor" % akkaV,
    "com.typesafe.akka" %% "akka-testkit" % akkaV % "it, test",
    "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
    "io.spray" %% "spray-can" % sprayV,
    "io.spray" %% "spray-routing" % sprayV,
    "io.spray" %% "spray-client" % sprayV,
    "io.spray" %% "spray-http" % sprayV,
    "io.spray" %% "spray-json" % "1.3.1",
    "org.webjars" % "swagger-ui" % "2.0.24",
    "io.spray" %% "spray-testkit" % sprayV % "it, test",
    "org.scalatest" %% "scalatest" % "2.2.4" % "it, test",
    "org.mock-server" % "mockserver-netty" % "3.9.2" % "test",
    "com.orientechnologies" % "orientdb-core" % "2.0.8",
    "com.orientechnologies" % "orientdb-graphdb" % "2.0.8",
    "com.orientechnologies" % "orientdb-server" % "2.0.8",
    "com.orientechnologies" % "orientdb-client" % "2.0.8",
    "com.tinkerpop.gremlin" % "gremlin-java" % "2.6.0",
    "org.broadinstitute" %% "cromwell" % "0.1-SNAPSHOT",
    ("com.google.apis" % "google-api-services-storage" % "v1-rev30-1.20.0")
      .exclude("com.google.guava", "guava-jdk5")
  )
}

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "typesafe", xs @ _*) => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

Revolver.settings

Revolver.enableDebugging(port = 5050, suspend = false)

Defaults.itSettings

lazy val rawls = project.in(file(".")).configs(IntegrationTest)