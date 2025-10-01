import sbt._

object Dependencies {
  val scalaV = "2.13"

  val akkaV         = "2.6.8"
  val akkaHttpV     = "10.2.0"
  val jacksonV      = "2.20.0"
  val jacksonAnnotationsV = "2.20"

  val workbenchLibsHash = "80e4b8d"
  val serviceTestV = s"5.0-${workbenchLibsHash}"
  val workbenchGoogleV = s"0.33-${workbenchLibsHash}"
  val workbenchGoogle2V = s"0.36-${workbenchLibsHash}"
  val workbenchModelV  = s"0.20-${workbenchLibsHash}"
  val workbenchMetricsV  = s"0.8-${workbenchLibsHash}"

  val workbenchModel: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-model" % workbenchModelV
  val workbenchMetrics: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-metrics" % workbenchMetricsV
  val workbenchExclusions = Seq(
    ExclusionRule(organization = "org.broadinstitute.dsde.workbench", name = "workbench-model_" + scalaV),
    ExclusionRule(organization = "org.broadinstitute.dsde.workbench", name = "workbench-util_" + scalaV),
    ExclusionRule(organization = "org.broadinstitute.dsde.workbench", name = "workbench-metrics_" + scalaV)
  )
  val rawlsModelExclusion = ExclusionRule(organization = "org.broadinstitute.dsde", name = "rawls-model_" + scalaV)

  val workbenchGoogle: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-google" % workbenchGoogleV excludeAll(workbenchExclusions:_*)
  // workbenchGoogle2 excludes slf4j because it pulls in too advanced a version
  val workbenchGoogle2: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-google2" % workbenchGoogle2V exclude ("org.slf4j", "slf4j-api")
  val workbenchServiceTest: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-service-test" % serviceTestV % "test" classifier "tests" excludeAll(workbenchExclusions :+ rawlsModelExclusion:_*)

  val rootDependencies = Seq(
    // proactively pull in latest versions of Jackson libs, instead of relying on the versions
    // specified as transitive dependencies, due to OWASP DependencyCheck warnings for earlier versions.
    "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonAnnotationsV,
    "com.fasterxml.jackson.core" % "jackson-databind" % jacksonV,
    "com.fasterxml.jackson.core" % "jackson-core" % jacksonV,
    "com.fasterxml.jackson.module" % ("jackson-module-scala_" + scalaV) % jacksonV,
    "ch.qos.logback" % "logback-classic" % "1.5.19",
    "net.logstash.logback" % "logstash-logback-encoder" % "6.6",
    "com.google.apis" % "google-api-services-oauth2" % "v1-rev112-1.22.0" excludeAll (
      ExclusionRule("com.google.guava", "guava-jdk5"),
      ExclusionRule("org.apache.httpcomponents", "httpclient")
    ),
    "com.google.api-client" % "google-api-client" % "1.22.0" excludeAll (
      ExclusionRule("com.google.guava", "guava-jdk5"),
      ExclusionRule("org.apache.httpcomponents", "httpclient")),
    "com.typesafe.akka"   %%  "akka-http-core"     % akkaHttpV,
    "com.typesafe.akka"   %%  "akka-stream-testkit" % akkaV,
    "com.typesafe.akka"   %%  "akka-http"           % akkaHttpV,
    "com.typesafe.akka"   %%  "akka-testkit"        % akkaV     % Test,
    "com.typesafe.akka"   %%  "akka-slf4j"          % akkaV,
    "org.scalatest"       %%  "scalatest"     % "3.2.2"   % Test,
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.6",
    "org.broadinstitute.dsde"       %% "rawls-model"         % "v0.0.537-SNAP"
      exclude("com.typesafe.scala-logging", "scala-logging_2.13")
      exclude("com.typesafe.akka", "akka-stream_2.13"),

    workbenchModel,
    workbenchMetrics,
    workbenchGoogle,
    workbenchGoogle2,
    workbenchServiceTest
  )

  val transitiveDependencyOverrides = Seq(
    // override cats-parse to address conflicting dependency versions for scala-uri
    "org.typelevel" %% "cats-parse" % "1.1.0"
  )
}
