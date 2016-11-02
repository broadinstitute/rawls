import Dependencies._
import Merging._
import Testing._
import Compiling._
import sbt.Keys._
import sbt._
import sbtassembly.AssemblyPlugin.autoImport._

object Settings {

  val artifactory = "https://artifactory.broadinstitute.org/artifactory/"

  val commonResolvers = List(
    "artifactory-releases" at artifactory + "libs-release",
    "artifactory-snapshots" at artifactory + "libs-snapshot"
  )

  val buildSettings: Seq[_root_.sbt.Def.Setting[_]] = Defaults.defaultSettings ++ Seq(
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

  val commonSettings: Seq[_root_.sbt.Def.Setting[_]] =
    buildSettings ++ assemblySettings ++ testSettings ++ List(
    organization  := "org.broadinstitute",
    scalaVersion  := "2.11.7",
    resolvers ++= commonResolvers,
    scalacOptions ++= compilerSettings
  )

  val modelSettings = List(
    name := "rawls-model",
    version := "0.1", //FIXME: do we ever increment this? Should we care?
    libraryDependencies ++= modelDependencies
  ) ++ commonSettings

  val rawlsSettings = List(
    name := "rawls-core",
    version := "0.1",
    libraryDependencies ++= rawlsDependencies
  ) ++ rawlsCompileSettings ++ commonSettings

  val rootSettings = List(
    name := "rawls",
    version := "0.1"
  )
}
