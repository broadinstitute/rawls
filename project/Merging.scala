import sbtassembly.{MergeStrategy, PathList}

object Merging {
  def customMergeStrategy(oldStrategy: (String) => MergeStrategy):(String => MergeStrategy) = {
    case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.discard
    case x if x.endsWith("Resource$AuthenticationType.class") => MergeStrategy.first
    case x if x.endsWith("module-info.class") => MergeStrategy.discard
    case PathList("org", "apache", _ @ _*) => MergeStrategy.last
    case PathList("com", "typesafe", _ @ _*) => MergeStrategy.last
    case PathList("com", "google", "auto", "value", _ @ _*) => MergeStrategy.last
    case PathList("io", "sundr", _ @ _*) => MergeStrategy.last
    // For the following error:
    //[error] java.lang.RuntimeException: deduplicate: different file contents found in the following:
    //[error] /root/.cache/coursier/v1/https/repo1.maven.org/maven2/com/google/protobuf/protobuf-java/3.11.4/protobuf-java-3.11.4.jar:google/protobuf/field_mask.proto
    //[error] /root/.cache/coursier/v1/https/repo1.maven.org/maven2/com/typesafe/akka/akka-protobuf-v3_2.12/2.6.5/akka-protobuf-v3_2.12-2.6.5.jar:google/protobuf/field_mask.proto
    case PathList("google", "protobuf", "field_mask.proto")         => MergeStrategy.first
    case PathList("google", "protobuf", "descriptor.proto")         => MergeStrategy.first
    case PathList("google", "protobuf", "compiler", "plugin.proto") => MergeStrategy.first
    case "application.conf" => MergeStrategy.first
    case "version.conf" => MergeStrategy.concat
    case "logback.xml" => MergeStrategy.first
    case "cobertura.properties" => MergeStrategy.discard
    case "overview.html" => MergeStrategy.first
    case x => oldStrategy(x)
  }
}
