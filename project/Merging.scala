import sbtassembly.{MergeStrategy, PathList}

object Merging {
  def customMergeStrategy(oldStrategy: (String) => MergeStrategy): (String => MergeStrategy) = {
    case x if x.endsWith("Resource$AuthenticationType.class") => MergeStrategy.first
    case x if x.endsWith("module-info.class")                 => MergeStrategy.discard
    case x if x.contains("bouncycastle")                      => MergeStrategy.first
    // For the following error:
    // [error] java.lang.RuntimeException: deduplicate: different file contents found in the following:
    // [error] /root/.cache/coursier/v1/https/repo1.maven.org/maven2/com/google/protobuf/protobuf-java/3.11.4/protobuf-java-3.11.4.jar:google/protobuf/field_mask.proto
    // [error] /root/.cache/coursier/v1/https/repo1.maven.org/maven2/com/typesafe/akka/akka-protobuf-v3_2.12/2.6.5/akka-protobuf-v3_2.12-2.6.5.jar:google/protobuf/field_mask.proto
    case PathList("google", "protobuf", _ @_*) => MergeStrategy.first
    // [error] /home/sbtuser/.cache/coursier/v1/https/repo1.maven.org/maven2/org/scala-lang/scala-library/2.12.15/scala-library-2.12.15.jar:scala/annotation/nowarn.class
    case PathList("scala", "annotation", _ @_*)          => MergeStrategy.first
    case PathList("javax", "ws", "rs", _ @_*)            => MergeStrategy.first
    case "version.conf"                                  => MergeStrategy.concat
    case "logback.xml"                                   => MergeStrategy.first
    case x if x.endsWith("kotlin_module")                => MergeStrategy.first
    case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.concat
    case x if x.endsWith("arrow-git.properties")         => MergeStrategy.concat
    case x                                               => oldStrategy(x)
  }
}
