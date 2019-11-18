import sbtassembly.{MergeStrategy, PathList}

object Merging {
  def customMergeStrategy(oldStrategy: (String) => MergeStrategy):(String => MergeStrategy) = {
    case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.discard
    case PathList("org", "apache", xs @ _*) => MergeStrategy.last
    case PathList("com", "typesafe", xs @ _*) => MergeStrategy.last
    case PathList("com", "google", "auto", "value", xs @ _*) => MergeStrategy.last
    case "application.conf" => MergeStrategy.first
    case "version.conf" => MergeStrategy.concat
    case "logback.xml" => MergeStrategy.first
    case "cobertura.properties" => MergeStrategy.discard
    case "overview.html" => MergeStrategy.first
    case x => oldStrategy(x)
  }
}
