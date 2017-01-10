import sbt._

object Dependencies {
  val akkaV = "2.3.6"
  val sprayV = "1.3.2"
  val slickV = "3.1.1"

  val modelDependencies = Seq(
    "io.spray" %% "spray-json" % "1.3.1",
    "io.spray" %% "spray-http" % sprayV,
    "joda-time" % "joda-time" % "2.9.4",
    "org.joda" % "joda-convert" % "1.8",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
    "org.scalatest" %% "scalatest" % "2.2.4" % "test"
  )

  val rawlsCoreDependencies = modelDependencies ++ Seq(
    "com.typesafe" % "config" % "1.3.0",
    "com.gettyimages" %% "spray-swagger" % "0.5.0"
      exclude("com.typesafe.scala-logging", "scala-logging-slf4j_2.11")
      exclude("com.typesafe.scala-logging", "scala-logging-api_2.11")
      exclude("com.google.guava", "guava"),
    "com.typesafe.akka" %% "akka-actor" % akkaV,
    "com.typesafe.slick" %% "slick" % "3.1.1",
    "io.spray" %% "spray-can" % sprayV,
    "io.spray" %% "spray-routing" % sprayV,
    "io.spray" %% "spray-client" % sprayV,
    "org.webjars" % "swagger-ui" % "2.1.1",
    "org.apache.commons" % "commons-jexl" % "2.1.1",
    "org.broadinstitute" %% "wdl4s" % "0.5" exclude("org.scalaz", "scalaz-core_2.11") exclude("io.spray", "spray-json_2.11"),
    "org.scalaz" % "scalaz-core_2.11" % "7.1.3",
    "org.broadinstitute.dsde.vault" %% "vault-common" % "0.1-15-bf74315",
    "com.google.apis" % "google-api-services-cloudbilling" % "v1-rev7-1.22.0"
      exclude("com.google.guava", "guava-jdk5"),
    "com.google.apis" % "google-api-services-genomics" % "v1-rev89-1.22.0"
      exclude("com.google.guava", "guava-jdk5"),
    "com.google.apis" % "google-api-services-storage" % "v1-rev35-1.20.0"
      exclude("com.google.guava", "guava-jdk5"),
    "com.google.apis" % "google-api-services-cloudresourcemanager" % "v1-rev7-1.22.0"
      exclude("com.google.guava", "guava-jdk5"),
    "com.google.apis" % "google-api-services-cloudbilling" % "v1-rev7-1.22.0"
      exclude("com.google.guava", "guava-jdk5"),
    "com.google.apis" % "google-api-services-compute" % "v1-rev72-1.20.0",
    "com.google.apis" % "google-api-services-admin-directory" % "directory_v1-rev53-1.20.0",
    "com.google.apis" % "google-api-services-plus" % "v1-rev381-1.20.0",
    "com.google.apis" % "google-api-services-oauth2" % "v1-rev112-1.20.0",
    "com.google.apis" % "google-api-services-pubsub" % "v1-rev14-1.22.0",
    "com.google.guava" % "guava" % "19.0",
    "com.typesafe.slick" %% "slick" % slickV,
    "com.typesafe.slick" %% "slick-hikaricp" % slickV,
    "mysql" % "mysql-connector-java" % "5.1.38",
    "org.liquibase" % "liquibase-core" % "3.5.3",
    "ch.qos.logback" % "logback-classic" % "1.1.6",
    "com.typesafe.akka" %% "akka-testkit" % akkaV % "test",
    "io.spray" %% "spray-testkit" % sprayV % "test",
    "org.mock-server" % "mockserver-netty" % "3.9.2" % "test"
  )
}
