import Dependencies._
import Merging._
import Testing._
import Compiling._
import Version._
import Publishing._
import sbt.Keys._
import sbt._
import sbtassembly.AssemblyPlugin.autoImport._

object Settings {

  val artifactory = "https://artifactory.broadinstitute.org/artifactory/"

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
    "-deprecation",
    "-encoding", "utf8",
    "-Xmax-classfile-name", "100"
  )

  //sbt assembly settings common to rawlsCore and rawlsModel
  val commonAssemblySettings = Seq(
    assemblyMergeStrategy in assembly := customMergeStrategy((assemblyMergeStrategy in assembly).value),
    test in assembly := {}
  )

  //sbt assembly settings for rawls proper needs to include a main class to launch the jar
  val rawlsAssemblySettings = Seq(
    mainClass in assembly := Some("org.broadinstitute.dsde.rawls.Boot")
  )

  //common settings for all sbt subprojects
  val commonSettings =
    commonBuildSettings ++ commonAssemblySettings ++ commonTestSettings ++ List(
    organization  := "org.broadinstitute",
    scalaVersion  := "2.11.8",
    resolvers ++= commonResolvers,
    scalacOptions ++= commonCompilerSettings
  )

  //the full list of settings for the rawlsModel project (see build.sbt)
  //commonSettings -> coreDefaultSettings sets name so we put it first so we can override it ourselves
  val modelSettings = commonSettings ++ List(
    name := "rawls-model",
    libraryDependencies ++= modelDependencies
  ) ++ modelVersionSettings ++ modelPublishSettings

  //the full list of settings for the rawlsCore project (see build.sbt)
  //commonSettings -> defaultSettings overrides name so we put it first
  val rawlsCoreSettings = commonSettings ++ List(
    name := "rawls-core",
    version := "0.1",
    libraryDependencies ++= rawlsCoreDependencies
  ) ++ rawlsAssemblySettings ++ noPublishSettings ++ rawlsCompileSettings
  //NOTE: rawlsCoreCompileSettings above has to be last, because something in commonSettings or rawlsAssemblySettings
  //overwrites it if it's before them. I (hussein) don't know what that is and I don't care to poke the bear to find out.

  //the full list of settings for the root rawls project that's ultimately the one we build into a fat JAR and run
  //commonSettings -> defaultSettings overrides name so we put it first
  val rootSettings = commonSettings ++ List(
    name := "rawls",
    version := "0.1"
  ) ++ rawlsAssemblySettings ++ noPublishSettings ++ rawlsCompileSettings
  //See immediately above NOTE.
}
