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
import org.scalafmt.sbt.ScalafmtPlugin.autoImport.scalafmtFilter

//noinspection TypeAnnotation
object Settings {
  val proxyResolvers = List(
    "internal-maven-proxy" at artifactory + "maven-central"
  )

  val commonResolvers = List(
    "artifactory-releases" at artifactory + "libs-release",
    "artifactory-snapshots" at artifactory + "libs-snapshot",
    Resolver.mavenLocal
  )

  //coreDefaultSettings + defaultConfigs = the now deprecated defaultSettings
  val commonBuildSettings = Defaults.coreDefaultSettings ++ Defaults.defaultConfigs ++ Seq(
    javaOptions += "-Xmx2G"
  )

  val java17BuildSettings = Seq(
    javacOptions ++= Seq("--release", "17")
  )

  def scalacOptionsVersion(scalaVersion: String) = {
    val commonCompilerSettings = Seq(
      "-deprecation:false", // This is tricky to enable as of 03/2020 [AEN]
      "-encoding", "utf8",
      "-feature",
      "-language:higherKinds",
      "-opt:l:inline",
      "-opt-inline-from:org.broadinstitute.dsde.rawls.**",
      "-unchecked",
      "-Xfatal-warnings"
    )

    val scala213CompilerSettings = Seq(
//      "-Ywarn-unused:imports"   // re-enable when imports optimised
    )

    commonCompilerSettings ++ (CrossVersion.partialVersion(scalaVersion) match {
      case Some((2, 13)) => scala213CompilerSettings
      case _ => Nil
    })
  }

  //sbt assembly settings common to rawlsCore and rawlsModel
  val commonAssemblySettings = Seq(
    assembly / assemblyMergeStrategy := customMergeStrategy((assembly / assemblyMergeStrategy).value),
    //  Try to fix the following error. We're not using akka-stream, so it should be safe to exclude `akka-protobuf`
    //  [error] /Users/qi/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/google/protobuf/protobuf-java/3.11.4/protobuf-java-3.11.4.jar:google/protobuf/field_mask.proto
    //  [error] /Users/qi/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/typesafe/akka/akka-protobuf-v3_2.12/2.6.1/akka-protobuf-v3_2.12-2.6.1.jar:google/protobuf/field_mask.proto
    assembly / assemblyExcludedJars := {
      val cp = (assembly / fullClasspath).value
      cp filter {_.data.getName == "akka-protobuf-v3_2.13-2.6.3.jar"}
    },
    assembly / test := {}
  )

  //sbt assembly settings for rawls proper needs to include a main class to launch the jar
  val rawlsAssemblySettings = Seq(
    assembly / mainClass := Some("org.broadinstitute.dsde.rawls.Boot"),
    // we never want guava-jdk5
    assembly / assemblyExcludedJars := {
      val cp = (assembly / fullClasspath).value
      cp filter {_.data.getName.startsWith("guava-jdk5")}
    }
  )

  val scalafmtSettings = List(
    Global / excludeLintKeys += scalafmtFilter,
    Global / scalafmtFilter := "diff-ref=HEAD^"
  )

  // When updating this, also update Docker image (https://hub.docker.com/r/sbtscala/scala-sbt/tags)
  val scala213 = "2.13.14"

  // common settings for all sbt subprojects, without enforcing that a database is present (for tests)
  val commonSettingsWithoutDb =
    commonBuildSettings ++ commonAssemblySettings ++ testSettingsWithoutDb ++ scalafmtSettings ++ List(
    organization  := "org.broadinstitute.dsde",
    scalaVersion  := scala213,
    resolvers := proxyResolvers ++: resolvers.value ++: commonResolvers,
    scalaVersion  := scala213,
    dependencyOverrides ++= transitiveDependencyOverrides,
    scalacOptions ++= scalacOptionsVersion(scalaVersion.value)
  )

  val commonSettings = commonSettingsWithoutDb ++ testSettingsWithDb

  //the full list of settings for the workbenchGoogle project (see build.sbt)
  //coreDefaultSettings (inside commonSettings) sets the project name, which we want to override, so ordering is important.
  //thus commonSettings needs to be added first.
  val googleSettings = commonSettings ++ List(
    name := "workbench-google",
    libraryDependencies ++= googleDependencies
  ) ++ versionSettings ++ noPublishSettings ++ java17BuildSettings

  //the full list of settings for the rawlsModel project (see build.sbt)
  //coreDefaultSettings (inside commonSettings) sets the project name, which we want to override, so ordering is important.
  //thus commonSettings needs to be added first.
  val modelSettings = commonSettings ++ List(
    name := "rawls-model",
    libraryDependencies ++= modelDependencies
  ) ++ versionSettings ++ publishSettings ++ java17BuildSettings

  //the full list of settings for the workbenchUtil project (see build.sbt)
  //coreDefaultSettings (inside commonSettings) sets the project name, which we want to override, so ordering is important.
  //thus commonSettings needs to be added first.
  val utilSettings = commonSettings ++ List(
    name := "workbench-util",
    libraryDependencies ++= utilDependencies
  ) ++ versionSettings ++ noPublishSettings ++ java17BuildSettings

  //the full list of settings for the workbenchMetrics project (see build.sbt)
  //coreDefaultSettings (inside commonSettings) sets the project name, which we want to override, so ordering is important.
  //thus commonSettings needs to be added first.
  val metricsSettings = commonSettings ++ List(
    name := "workbench-metrics",
    libraryDependencies ++= metricsDependencies
  ) ++ versionSettings ++ noPublishSettings ++ java17BuildSettings

  //the full list of settings for the rawlsCore project (see build.sbt)
  //coreDefaultSettings (inside commonSettings) sets the project name, which we want to override, so ordering is important.
  //thus commonSettings needs to be added first.
  val rawlsCoreSettings = commonSettings ++ List(
    name := "rawls-core",
    libraryDependencies ++= rawlsCoreDependencies
  ) ++ versionSettings ++ antlr4CodeGenerationSettings ++ rawlsAssemblySettings ++ noPublishSettings ++ rawlsCompileSettings ++ java17BuildSettings
  //NOTE: rawlsCoreCompileSettings above has to be last, because something in commonSettings or rawlsAssemblySettings
  //overwrites it if it's before them. I (hussein) don't know what that is and I don't care to poke the bear to find out.

  //the full list of settings for the root rawls project that's ultimately the one we build into a fat JAR and run
  //coreDefaultSettings (inside commonSettings) sets the project name, which we want to override, so ordering is important.
  //thus commonSettings needs to be added first.
  val rootSettings = commonSettings ++ List(
    name := "rawls",
  ) ++ versionSettings ++ rawlsAssemblySettings ++ noPublishSettings ++ rawlsCompileSettings ++ java17BuildSettings
  //See immediately above NOTE.

  val pact4sSettings = commonSettingsWithoutDb ++ List(
    libraryDependencies ++= pact4sDependencies,

    /**
      * Invoking pact tests from root project (sbt "project pact" test)
      * will launch tests in a separate JVM context that ensures contracts
      * are written to the pact4s/target/pacts folder. Otherwise, contracts
      * will be written to the root folder.
      */
    Test / fork := true

  ) ++ commonAssemblySettings ++ versionSettings
}
