import sbt.{SettingKey, _}
import Keys._
import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.{MergeStrategy, PathList}

object Merging {
  def customMergeStrategy(oldStrategy: (String) => MergeStrategy):(String => MergeStrategy) = {
    case PathList("org", "apache", xs @ _*) => MergeStrategy.last
    case PathList("com", "typesafe", xs @ _*) => MergeStrategy.last
    case "application.conf" => MergeStrategy.first
    case "logback.xml" => MergeStrategy.first
    case "cobertura.properties" => MergeStrategy.discard
    case "overview.html" => MergeStrategy.first
    case x => oldStrategy(x)
  }
}
