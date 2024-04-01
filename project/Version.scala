import sbt.Keys._
import sbt.Setting

import scala.sys.process._

object Version {
  def getVersionString = {
    def getLastCommitHashFromGit = s"""git log -n 1 --pretty=format:%h""" !!

    // the terra-github-workflows/rawls-build GHA workflow sets BUILD_NUMBER, GIT_COMMIT, and DOCKER_TAG environment variables
    val version = sys.env.getOrElse("DOCKER_TAG", getLastCommitHashFromGit).trim()

    // The project isSnapshot string passed in via command line settings, if desired.
    val isSnapshot = sys.props.getOrElse("project.isSnapshot", "true").toBoolean

    // For now, obfuscate SNAPSHOTs from sbt's developers: https://github.com/sbt/sbt/issues/2687#issuecomment-236586241
    if (isSnapshot) s"$version-SNAP" else version
  }

  val versionSettings: Seq[Setting[_]] =
    Seq(version := getVersionString)
}
