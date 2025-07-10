//import Artifactory.*
import sbt.Keys.*
import sbt._

/**
  * NOTE: This was lifted wholesale from Cromwell.
  */

object Publishing {
//  private val buildTimestamp = System.currentTimeMillis() / 1000

  private val garBase = "artifactregistry://us-central1-maven.pkg.dev/dsp-artifact-registry/"

  private def garResolver(): Resolver = {
    val isSnapshot = sys.props.getOrElse("project.isSnapshot", "false").toBoolean
    val repoType = if (isSnapshot) "snapshot" else "release"
    val repoUrl = s"${garBase}libs-$repoType-standard"
    val repoName = "gar-publish"
    repoName at repoUrl
  }

//  private def artifactoryResolver(isSnapshot: Boolean): Resolver = {
//    val repoType = if (isSnapshot) "snapshot" else "release"
//    val repoUrl =
//      s"${artifactory}libs-$repoType-local;build.timestamp=$buildTimestamp"
//    val repoName = "artifactory-publish"
//    repoName at repoUrl
//  }

//  private val artifactoryCredentials: Credentials = {
//    val username = sys.env.getOrElse("ARTIFACTORY_USERNAME", "")
//    val password = sys.env.getOrElse("ARTIFACTORY_PASSWORD", "")
//    Credentials("Artifactory Realm", artifactoryHost, username, password)
//  }

  val publishSettings: Seq[Setting[_]] = Seq(
    publishTo := Some(garResolver()), // Use release repo
    Compile / publishArtifact := true,
    Test / publishArtifact := true
  )

//  val publishSettings: Seq[Setting[_]] =
//    // we only publish to libs-release-local because of a bug in sbt that makes snapshots take
//    // priority over the local package cache. see here: https://github.com/sbt/sbt/issues/2687#issuecomment-236586241
//    Seq(
//      publishTo := Option(artifactoryResolver(false)),
//      credentials += artifactoryCredentials,
//      publishConfiguration := publishConfiguration.value.withOverwrite(true)
//    )

  val noPublishSettings: Seq[Setting[_]] =
    Seq(
      publish := {},
      publishLocal := {}
    )
}
