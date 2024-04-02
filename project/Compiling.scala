import sbt.Keys._
import sbt._

object Compiling {
  // generate version.conf
  val writeVersionConf = Def.task {
    val file = (Compile / resourceManaged).value / "version.conf"
    // the terra-github-workflows/rawls-build GHA workflow sets BUILD_NUMBER, GIT_COMMIT, and DOCKER_TAG environment variables
    val buildNumber = sys.env.getOrElse("BUILD_NUMBER", default = "None")
    val gitHash = sys.env.getOrElse("GIT_COMMIT", default = "None")
    val versionString = sys.env.getOrElse("DOCKER_TAG", version.value)
    val contents = "version {\nbuild.number=%s\ngit.hash=%s\nversion=%s\n}".format(buildNumber, gitHash, versionString)
    IO.write(file, contents)
    Seq(file)
  }

  val rawlsCompileSettings = List(
    Compile / resourceGenerators += writeVersionConf
  )
}
