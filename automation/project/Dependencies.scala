import sbt._

object Dependencies {
  val scalaV = "2.12"

  val jacksonV = "2.8.4"
  val akkaV = "2.5.7"
  val akkaHttpV = "10.0.10"

  val serviceTestV = "0.18-4f1d1951-SNAP"
  val workbenchGoogleV = "0.21-890a74f"
  val workbenchGoogle2V = "0.6-c91d96b"
  val workbenchModelV  = "0.13-58c913d"
  val workbenchMetricsV  = "0.5-4c7acd5"

  val workbenchModel: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-model" % workbenchModelV
  val workbenchMetrics: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-metrics" % workbenchMetricsV
  val workbenchExclusions = Seq(
    ExclusionRule(organization = "org.broadinstitute.dsde.workbench", name = "workbench-model_" + scalaV),
    ExclusionRule(organization = "org.broadinstitute.dsde.workbench", name = "workbench-util_" + scalaV),
    ExclusionRule(organization = "org.broadinstitute.dsde.workbench", name = "workbench-metrics_" + scalaV)
  )

  val workbenchGoogle: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-google" % workbenchGoogleV excludeAll(workbenchExclusions:_*)
  val workbenchGoogle2: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-google2" % workbenchGoogle2V
  val workbenchServiceTest: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-service-test" % serviceTestV % "test" classifier "tests" excludeAll(workbenchExclusions:_*)

  val rootDependencies = Seq(
    // proactively pull in latest versions of Jackson libs, instead of relying on the versions
    // specified as transitive dependencies, due to OWASP DependencyCheck warnings for earlier versions.
    "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonV,
    "com.fasterxml.jackson.core" % "jackson-databind" % jacksonV,
    "com.fasterxml.jackson.core" % "jackson-core" % jacksonV,
    "com.fasterxml.jackson.module" % ("jackson-module-scala_" + scalaV) % jacksonV,
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "com.google.apis" % "google-api-services-oauth2" % "v1-rev112-1.20.0" excludeAll (
      ExclusionRule("com.google.guava", "guava-jdk5"),
      ExclusionRule("org.apache.httpcomponents", "httpclient")
    ),
    "com.google.api-client" % "google-api-client" % "1.22.0" excludeAll (
      ExclusionRule("com.google.guava", "guava-jdk5"),
      ExclusionRule("org.apache.httpcomponents", "httpclient")),
    "com.typesafe.akka"   %%  "akka-http-core"     % akkaHttpV,
    "com.typesafe.akka"   %%  "akka-stream-testkit" % akkaV,
    "com.typesafe.akka"   %%  "akka-http"           % akkaHttpV,
    "com.typesafe.akka"   %%  "akka-testkit"        % akkaV     % "test",
    "com.typesafe.akka"   %%  "akka-slf4j"          % akkaV,
    "org.specs2"          %%  "specs2-core"   % "3.8.6"  % "test",
    "org.scalatest"       %%  "scalatest"     % "3.2.0"   % "test",
    "org.seleniumhq.selenium" % "selenium-java" % "3.8.1" % "test",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
    "org.broadinstitute.dsde"       %% "rawls-model"         % "0.1-0d02c8ce-SNAP"
      exclude("com.typesafe.scala-logging", "scala-logging_2.11") exclude("com.typesafe.akka", "akka-stream_2.11"),

    workbenchModel,
    workbenchMetrics,
    workbenchGoogle,
    workbenchGoogle2,
    workbenchServiceTest,

    // required by workbenchGoogle
    "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.6" % "provided"
  )
}
