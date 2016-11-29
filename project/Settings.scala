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

  val buildSettings = Defaults.defaultSettings ++ Seq(
    javaOptions += "-Xmx2G"
  )

  val compilerSettings: Seq[String] = Seq(
    "-unchecked",
    "-deprecation",
    "-encoding", "utf8",
    "-Xmax-classfile-name", "100"
  )

  val assemblySettings = Seq(
    assemblyMergeStrategy in assembly := customMergeStrategy((assemblyMergeStrategy in assembly).value),
    test in assembly := {}
  )

  val rawlsAssemblySettings = Seq(
    mainClass in assembly := Some("org.broadinstitute.dsde.rawls.Boot")
  )

  val commonSettings =
    buildSettings ++ assemblySettings ++ testSettings ++ List(
    organization  := "org.broadinstitute",
    scalaVersion  := "2.11.7",
    resolvers ++= commonResolvers,
    scalacOptions ++= compilerSettings
  )

  val modelSettings = List(
    name := "rawls-model",
    libraryDependencies ++= modelDependencies
  ) ++ commonSettings ++ modelVersionSettings ++ modelPublishSettings

  val rawlsSettings = List(
    name := "rawls-core",
    version := "0.1",
    libraryDependencies ++= rawlsDependencies
  ) ++ commonSettings ++ rawlsAssemblySettings ++ rawlsCompileSettings
  //NOTE: rawlsCompileSettings above has to be last, because something in commonSettings or rawlsAssemblySettings
  //overwrites it if it's before them. I (hussein) don't know what that is and I don't care to poke the bear to find out.

  val rootSettings = List(
    name := "rawls",
    version := "0.1"
  ) ++ commonSettings ++ rawlsAssemblySettings ++ rawlsCompileSettings
  //See immediately above NOTE.
}
