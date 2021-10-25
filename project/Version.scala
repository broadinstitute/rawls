import sbt.Keys._
import sbt.Setting

import scala.sys.process._

object Version {
  val baseModelVersion = "0.1"

  def getVersionString = {
    def getLastCommitHashFromGit = { s"""git log -n 1 --pretty=format:%h""" !! }
    //jenkins builds will pass the last commit hash in as an env variable because we currently build rawls
    //inside a docker container that doesn't know the code is a git repo.
    // if building from the hseeberger/scala-sbt docker image use env var (since hseeberger/scala-sbt doesn't have git in it)
    val gitHash = sys.env.getOrElse("GIT_HASH", getLastCommitHashFromGit).trim()
    val version = baseModelVersion + "-" + gitHash

    // The project isSnapshot string passed in via command line settings, if desired.
    val isSnapshot = sys.props.getOrElse("project.isSnapshot", "true").toBoolean

    // For now, obfuscate SNAPSHOTs from sbt's developers: https://github.com/sbt/sbt/issues/2687#issuecomment-236586241
    if (isSnapshot) s"$version-SNAP" else version
  }

  val versionSettings: Seq[Setting[_]] =
    Seq(version := getVersionString)
}
