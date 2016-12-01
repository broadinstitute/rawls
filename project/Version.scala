import sbt.Keys._
import sbt._

object Version {
  val baseModelVersion = "0.1"

  def getVersionString(baseDir: File) = {
    def getLastModelCommitFromGit = { s"""git log -n 1 --pretty=format:%h ${baseDir.getAbsolutePath}""" !! }

    //jenkins builds will pass the last model commit in as an env variable because we currently build rawls
    //inside a docker container that doesn't know the code is a git repo.
    //normal "developer" builds should be building from inside the rawls git repo so we can fall back to git if the env isn't specified.
    val lastModelCommit = sys.env.getOrElse("GIT_MODEL_HASH", getLastModelCommitFromGit ).trim()
    val version = baseModelVersion + "-" + lastModelCommit

    // The project isSnapshot string passed in via command line settings, if desired.
    val isSnapshot = sys.props.getOrElse("project.isSnapshot", "true").toBoolean

    // For now, obfuscate SNAPSHOTs from sbt's developers: https://github.com/sbt/sbt/issues/2687#issuecomment-236586241
    if (isSnapshot) s"$version-SNAP" else version
  }

  val modelVersionSettings: Seq[Setting[_]] =
    Seq(version := getVersionString(baseDirectory.value))

}
