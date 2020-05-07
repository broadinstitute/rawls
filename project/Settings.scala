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

  val commonResolvers = List(
    "artifactory-releases" at artifactory + "libs-release",
    "artifactory-snapshots" at artifactory + "libs-snapshot"
  )

  //coreDefaultSettings + defaultConfigs = the now deprecated defaultSettings
  val commonBuildSettings = Defaults.coreDefaultSettings ++ Defaults.defaultConfigs ++ Seq(
    javaOptions += "-Xmx2G"
  )

  val commonCompilerSettings = Seq(
    "-unchecked",
    "-feature",
    "-encoding", "utf8",
    "-Xmax-classfile-name", "100",
    "-Ywarn-unused-import",
    "-deprecation:false", // This is tricky to enable as of 03/2020 [AEN]
    "-Xfatal-warnings"
  )

  //sbt assembly settings common to rawlsCore and rawlsModel
  val commonAssemblySettings = Seq(
    assemblyMergeStrategy in assembly := customMergeStrategy((assemblyMergeStrategy in assembly).value),
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

  //common settings for all sbt subprojects
  val commonSettings =
    commonBuildSettings ++ commonAssemblySettings ++ commonTestSettings ++ List(
    organization  := "org.broadinstitute.dsde",
    scalaVersion  := "2.12.10",
    resolvers ++= commonResolvers,
    scalacOptions ++= commonCompilerSettings
  )

  //the full list of settings for the workbenchGoogle project (see build.sbt)
  //coreDefaultSettings (inside commonSettings) sets the project name, which we want to override, so ordering is important.
  //thus commonSettings needs to be added first.
  val googleSettings = commonSettings ++ List(
    name := "workbench-google",
    libraryDependencies ++= googleDependencies
  ) ++ versionSettings ++ publishSettings

  //the full list of settings for the rawlsModel project (see build.sbt)
  //coreDefaultSettings (inside commonSettings) sets the project name, which we want to override, so ordering is important.
  //thus commonSettings needs to be added first.
  val modelSettings = commonSettings ++ List(
    name := "rawls-model",
    libraryDependencies ++= modelDependencies
  ) ++ versionSettings ++ publishSettings

  //the full list of settings for the workbenchUtil project (see build.sbt)
  //coreDefaultSettings (inside commonSettings) sets the project name, which we want to override, so ordering is important.
  //thus commonSettings needs to be added first.
  val utilSettings = commonSettings ++ List(
    name := "workbench-util",
    libraryDependencies ++= utilDependencies
  ) ++ versionSettings ++ publishSettings

  //the full list of settings for the workbenchMetrics project (see build.sbt)
  //coreDefaultSettings (inside commonSettings) sets the project name, which we want to override, so ordering is important.
  //thus commonSettings needs to be added first.
  val metricsSettings = commonSettings ++ List(
    name := "workbench-metrics",
    libraryDependencies ++= metricsDependencies
  ) ++ versionSettings ++ publishSettings

  //the full list of settings for the rawlsCore project (see build.sbt)
  //coreDefaultSettings (inside commonSettings) sets the project name, which we want to override, so ordering is important.
  //thus commonSettings needs to be added first.
  val rawlsCoreSettings = commonSettings ++ List(
    name := "rawls-core",
    version := "0.1",
    libraryDependencies ++= rawlsCoreDependencies
  ) ++ antlr4CodeGenerationSettings ++ rawlsAssemblySettings ++ noPublishSettings ++ rawlsCompileSettings
  //NOTE: rawlsCoreCompileSettings above has to be last, because something in commonSettings or rawlsAssemblySettings
  //overwrites it if it's before them. I (hussein) don't know what that is and I don't care to poke the bear to find out.

  //the full list of settings for the root rawls project that's ultimately the one we build into a fat JAR and run
  //coreDefaultSettings (inside commonSettings) sets the project name, which we want to override, so ordering is important.
  //thus commonSettings needs to be added first.
  val rootSettings = commonSettings ++ List(
    name := "rawls",
    version := "0.1"
  ) ++ rawlsAssemblySettings ++ noPublishSettings ++ rawlsCompileSettings
  //See immediately above NOTE.
}
