import Artifactory._
import CodeGeneration._
import Compiling._
import Dependencies._
import Merging._
import Publishing._
import Testing._
import Version._
import sbt.Keys._
import sbt._
import sbtassembly.AssemblyPlugin.autoImport._

//noinspection TypeAnnotation
object Settings {

  val proxyResolvers = List(
    "internal-maven-proxy" at artifactory + "maven-central"
  )

  val commonResolvers = List(
    "artifactory-releases" at artifactory + "libs-release",
    "artifactory-snapshots" at artifactory + "libs-snapshot"
  )

  //coreDefaultSettings + defaultConfigs = the now deprecated defaultSettings
  val commonBuildSettings = Defaults.coreDefaultSettings ++ Defaults.defaultConfigs ++ Seq(
    javaOptions += "-Xmx2G"
  )

  val java11BuildSettings = Seq( // can be wrapped into commonBuildSettings when rawls-model can publish java 11
    javacOptions ++= Seq("--release", "11")
  )

  val commonCompilerSettings = Seq(
    "-unchecked",
    "-feature",
    "-encoding", "utf8",
//    "-Ywarn-unused-import", bad option for 2.13
    "-deprecation:false", // This is tricky to enable as of 03/2020 [AEN]
    "-Xfatal-warnings"
  )

  //sbt assembly settings common to rawlsCore and rawlsModel
  val commonAssemblySettings = Seq(
    assemblyMergeStrategy in assembly := customMergeStrategy((assemblyMergeStrategy in assembly).value),
    //  Try to fix the following error. We're not using akka-stream, so it should be safe to exclude `akka-protobuf`
    //  [error] /Users/qi/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/google/protobuf/protobuf-java/3.11.4/protobuf-java-3.11.4.jar:google/protobuf/field_mask.proto
    //  [error] /Users/qi/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/typesafe/akka/akka-protobuf-v3_2.12/2.6.1/akka-protobuf-v3_2.12-2.6.1.jar:google/protobuf/field_mask.proto
    assemblyExcludedJars in assembly := {
      val cp = (fullClasspath in assembly).value
      cp filter {_.data.getName == "akka-protobuf-v3_2.12-2.6.3.jar"}
    },
    test in assembly := {}
  )

  //sbt assembly settings for rawls proper needs to include a main class to launch the jar
  val rawlsAssemblySettings = Seq(
    mainClass in assembly := Some("org.broadinstitute.dsde.rawls.Boot"),
    // we never want guava-jdk5
    assemblyExcludedJars in assembly := {
      val cp = (fullClasspath in assembly).value
      cp filter {_.data.getName.startsWith("guava-jdk5")}
    }
  )

  val cross212and213 = Seq(
    crossScalaVersions := List("2.12.15", "2.13.2")
  )

  //common settings for all sbt subprojects
  val commonSettings =
    commonBuildSettings ++ commonAssemblySettings ++ commonTestSettings ++ List(
    organization  := "org.broadinstitute.dsde",
    scalaVersion  := "2.12.15", // `cromwell-client` needs to support 2.13 for rawls to be able to upgrade
    resolvers := proxyResolvers ++: resolvers.value ++: commonResolvers,
    scalacOptions ++= commonCompilerSettings
  )

  //the full list of settings for the workbenchGoogle project (see build.sbt)
  //coreDefaultSettings (inside commonSettings) sets the project name, which we want to override, so ordering is important.
  //thus commonSettings needs to be added first.
  val googleSettings = commonSettings ++ List(
    name := "workbench-google",
    libraryDependencies ++= googleDependencies
  ) ++ versionSettings ++ noPublishSettings ++ java11BuildSettings

  //the full list of settings for the rawlsModel project (see build.sbt)
  //coreDefaultSettings (inside commonSettings) sets the project name, which we want to override, so ordering is important.
  //thus commonSettings needs to be added first.
  val modelSettings = cross212and213 ++ commonSettings ++ List(
    name := "rawls-model",
    javacOptions ++= Seq("--release", "8"), // has to publish a java 8 artifact
    libraryDependencies ++= modelDependencies
  ) ++ versionSettings ++ publishSettings

  //the full list of settings for the workbenchUtil project (see build.sbt)
  //coreDefaultSettings (inside commonSettings) sets the project name, which we want to override, so ordering is important.
  //thus commonSettings needs to be added first.
  val utilSettings = commonSettings ++ List(
    name := "workbench-util",
    libraryDependencies ++= utilDependencies
  ) ++ versionSettings ++ noPublishSettings ++ java11BuildSettings

  //the full list of settings for the workbenchMetrics project (see build.sbt)
  //coreDefaultSettings (inside commonSettings) sets the project name, which we want to override, so ordering is important.
  //thus commonSettings needs to be added first.
  val metricsSettings = commonSettings ++ List(
    name := "workbench-metrics",
    libraryDependencies ++= metricsDependencies
  ) ++ versionSettings ++ noPublishSettings ++ java11BuildSettings

  //the full list of settings for the rawlsCore project (see build.sbt)
  //coreDefaultSettings (inside commonSettings) sets the project name, which we want to override, so ordering is important.
  //thus commonSettings needs to be added first.
  val rawlsCoreSettings = commonSettings ++ List(
    name := "rawls-core",
    version := "0.1",
    libraryDependencies ++= rawlsCoreDependencies
  ) ++ antlr4CodeGenerationSettings ++ rawlsAssemblySettings ++ noPublishSettings ++ rawlsCompileSettings ++ java11BuildSettings
  //NOTE: rawlsCoreCompileSettings above has to be last, because something in commonSettings or rawlsAssemblySettings
  //overwrites it if it's before them. I (hussein) don't know what that is and I don't care to poke the bear to find out.

  //the full list of settings for the root rawls project that's ultimately the one we build into a fat JAR and run
  //coreDefaultSettings (inside commonSettings) sets the project name, which we want to override, so ordering is important.
  //thus commonSettings needs to be added first.
  val rootSettings = commonSettings ++ List(
    name := "rawls",
    version := "0.1"
  ) ++ rawlsAssemblySettings ++ noPublishSettings ++ rawlsCompileSettings ++ java11BuildSettings
  //See immediately above NOTE.
}
