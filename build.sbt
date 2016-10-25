import sbt.ExclusionRule

name          := "rawls"

organization  := "org.broadinstitute"

version       := "0.1"

scalaVersion  := "2.11.7"

scalacOptions := Seq(
  "-unchecked",
  "-deprecation",
  "-encoding", "utf8",
  "-Xmax-classfile-name", "100"
)

val artifactory = "https://artifactory.broadinstitute.org/artifactory/"

resolvers += "artifactory-releases" at artifactory + "libs-release"

resolvers += "artifactory-snapshots" at artifactory + "libs-snapshot"

libraryDependencies ++= {
  val akkaV = "2.3.6"
  val sprayV = "1.3.2"
  val slickV = "3.1.1"
  Seq(
    "com.typesafe" % "config" % "1.3.0",
    ("com.gettyimages" %% "spray-swagger" % "0.5.0").exclude("com.typesafe.scala-logging", "scala-logging-slf4j_2.11").exclude("com.typesafe.scala-logging", "scala-logging-api_2.11").exclude("com.google.guava", "guava"),
    "com.typesafe.akka" %% "akka-actor" % akkaV,
    "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
    "com.typesafe.slick" %% "slick" % "3.1.1",
    "io.spray" %% "spray-can" % sprayV,
    "io.spray" %% "spray-routing" % sprayV,
    "io.spray" %% "spray-client" % sprayV,
    "io.spray" %% "spray-http" % sprayV,
    "io.spray" %% "spray-json" % "1.3.1",
    "org.webjars" % "swagger-ui" % "2.1.1",
    "org.apache.commons" % "commons-jexl" % "2.1.1",
    ("org.broadinstitute" %% "wdl4s" % "0.4"),
    "org.broadinstitute.dsde.vault" %% "vault-common" % "0.1-15-bf74315",
    ("com.google.apis" % "google-api-services-cloudbilling" % "v1-rev7-1.22.0").exclude("com.google.guava", "guava-jdk5"),
    ("com.google.apis" % "google-api-services-genomics" % "v1-rev89-1.22.0").exclude("com.google.guava", "guava-jdk5"),
    ("com.google.apis" % "google-api-services-storage" % "v1-rev35-1.20.0").exclude("com.google.guava", "guava-jdk5"),
    ("com.google.apis" % "google-api-services-cloudresourcemanager" % "v1-rev7-1.22.0").exclude("com.google.guava", "guava-jdk5"),
    ("com.google.apis" % "google-api-services-cloudbilling" % "v1-rev7-1.22.0").exclude("com.google.guava", "guava-jdk5"),
    ("com.google.apis" % "google-api-services-compute" % "v1-rev72-1.20.0"),
    ("com.google.apis" % "google-api-services-plus" % "v1-rev381-1.20.0"),
    ("com.google.apis" % "google-api-services-admin-directory" % "directory_v1-rev53-1.20.0"),
    "com.google.apis" % "google-api-services-oauth2" % "v1-rev112-1.20.0",
    "com.google.guava" % "guava" % "19.0",
    "com.typesafe.slick" %% "slick" % slickV,
    "com.typesafe.slick" %% "slick-hikaricp" % slickV,
    "mysql" % "mysql-connector-java" % "5.1.38",
    "org.liquibase" % "liquibase-core" % "3.3.5",
    "ch.qos.logback" % "logback-classic" % "1.1.6",
    "com.typesafe.akka" %% "akka-testkit" % akkaV % "test",
    "io.spray" %% "spray-testkit" % sprayV % "test",
    "org.scalatest" %% "scalatest" % "2.2.4" % "test",
    "org.mock-server" % "mockserver-netty" % "3.9.2" % "test"
  )
}

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "typesafe", xs @ _*) => MergeStrategy.last
  case "application.conf" => MergeStrategy.first
  case "logback.xml" => MergeStrategy.first
  case "cobertura.properties" => MergeStrategy.discard
  case "overview.html" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

Revolver.settings

Revolver.enableDebugging(port = 5050, suspend = false)

// When JAVA_OPTS are specified in the environment, they are usually meant for the application
// itself rather than sbt, but they are not passed by default to the application, which is a forked
// process. This passes them through to the "re-start" command, which is probably what a developer
// would normally expect.
javaOptions in Revolver.reStart := sys.env("JAVA_OPTS").split(" ")

net.virtualvoid.sbt.graph.Plugin.graphSettings

def isIntegrationTest(name: String) = name contains "integrationtest"

lazy val IntegrationTest = config ("it") extend (Test)

lazy val validMysqlHost = taskKey[Unit]("Determine if mysql.host is provided.")

validMysqlHost := {
  val hostName = sys.props.getOrElse("mysql.host", "")
  if (hostName.length == 0) {
    val log = streams.value.log
    log.error("Database host name not set. Please see run instructions in README.md for providing a valid database hostname")
    sys.exit()
  }
}

lazy val rawls = project.in(file("."))
  .configs(IntegrationTest)
  .settings(inConfig(IntegrationTest)(Defaults.testTasks): _*)
  .settings((test in Test) <<= (test in Test) dependsOn validMysqlHost)
  .settings((testOnly in Test) <<= (testOnly in Test) dependsOn validMysqlHost)
  .settings(
    testOptions in Test ++= Seq(Tests.Filter(s => !isIntegrationTest(s))),
    testOptions in IntegrationTest := Seq(Tests.Filter(s => isIntegrationTest(s)))
  )

// SLF4J initializes itself upon the first logging call.  Because sbt
// runs tests in parallel it is likely that a second thread will
// invoke a second logging call before SLF4J has completed
// initialization from the first thread's logging call, leading to
// these messages:
//   SLF4J: The following loggers will not work because they were created
//   SLF4J: during the default configuration phase of the underlying logging system.
//   SLF4J: See also http://www.slf4j.org/codes.html#substituteLogger
//   SLF4J: com.imageworks.common.concurrent.SingleThreadInfiniteLoopRunner
//
// As a workaround, load SLF4J's root logger before starting the unit
// tests

// Source: https://github.com/typesafehub/scalalogging/issues/23#issuecomment-17359537
// References:
//   http://stackoverflow.com/a/12095245
//   http://jira.qos.ch/browse/SLF4J-167
//   http://jira.qos.ch/browse/SLF4J-97

parallelExecution in Test := false

testOptions in Test += Tests.Setup(classLoader =>
  classLoader
    .loadClass("org.slf4j.LoggerFactory")
    .getMethod("getLogger", classLoader.loadClass("java.lang.String"))
    .invoke(null, "ROOT")
)

test in assembly := {}

val buildSettings = Defaults.defaultSettings ++ Seq(
  javaOptions += "-Xmx2G"
)

// generate version.conf
resourceGenerators in Compile <+= Def.task {
  val file = (resourceManaged in Compile).value / "version.conf"
  // jenkins sets BUILD_NUMBER and GIT_COMMIT environment variables
  val buildNumber = sys.env.getOrElse("BUILD_NUMBER", default = "None")
  val gitHash = sys.env.getOrElse("GIT_COMMIT", default = "None")
  val contents = "version {\nbuild.number=%s\ngit.hash=%s\nversion=%s\n}".format(buildNumber, gitHash, version.value)
  IO.write(file, contents)
  Seq(file)
}
