import sbt.ExclusionRule

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
    "com.typesafe.akka" %% "akka-testkit" % akkaV % "test",
    "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
    "io.spray" %% "spray-can" % sprayV,
    "io.spray" %% "spray-routing" % sprayV,
    "io.spray" %% "spray-client" % sprayV,
    "io.spray" %% "spray-http" % sprayV,
    "io.spray" %% "spray-json" % "1.3.1",
    "org.webjars" % "swagger-ui" % "2.0.24",
    "io.spray" %% "spray-testkit" % sprayV % "test",
    "org.scalatest" %% "scalatest" % "2.2.4" % "test",
    "org.mock-server" % "mockserver-netty" % "3.9.2" % "test",
    "com.orientechnologies" % "orientdb-core" % "2.0.8",
    "com.orientechnologies" % "orientdb-graphdb" % "2.0.8",
    "com.orientechnologies" % "orientdb-server" % "2.0.8",
    "com.orientechnologies" % "orientdb-client" % "2.0.8",
    "com.tinkerpop.gremlin" % "gremlin-java" % "2.6.0",
    "org.apache.commons" % "commons-jexl" % "2.1.1",
    "rhino" % "js" % "1.7R2",
    ("org.broadinstitute" %% "cromwell" % "0.9")
      excludeAll (ExclusionRule(organization = "com.gettyimages"), ExclusionRule(organization = "org.webjars")) ,
    "org.broadinstitute.dsde.vault" %% "vault-common" % "0.1-15-bf74315",
    ("com.google.apis" % "google-api-services-storage" % "v1-rev30-1.20.0").exclude("com.google.guava", "guava-jdk5"),
    ("com.google.apis" % "google-api-services-compute" % "v1-rev72-1.20.0"),
    ("com.google.apis" % "google-api-services-admin-directory" % "directory_v1-rev53-1.20.0")
  )
}

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "typesafe", xs @ _*) => MergeStrategy.last
  case "application.conf" => MergeStrategy.first
  case "logback.xml" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

Revolver.settings

Revolver.enableDebugging(port = 5050, suspend = false)

def isIntegrationTest(name: String) = name contains "integrationtest"

lazy val IntegrationTest = config ("it") extend (Test)

lazy val rawls = project.in(file("."))
  .configs(IntegrationTest)
  .settings(inConfig(IntegrationTest)(Defaults.testTasks): _*)
  .settings(
    testOptions in Test ++= Seq(Tests.Filter(s => !isIntegrationTest(s))),
    testOptions in IntegrationTest := Seq(Tests.Filter(s => isIntegrationTest(s)))
  )
