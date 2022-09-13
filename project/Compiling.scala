import sbt.Keys._
import sbt._

object Compiling {
  // generate version.conf
  val writeVersionConf = Def.task {
    val file = (Compile / resourceManaged).value / "version.conf"
    // jenkins sets BUILD_NUMBER and GIT_COMMIT environment variables
    val buildNumber = sys.env.getOrElse("BUILD_NUMBER", default = "None")
    val gitHash = sys.env.getOrElse("GIT_COMMIT", default = "None")
    val contents = "version {\nbuild.number=%s\ngit.hash=%s\nversion=%s\n}".format(buildNumber, gitHash, version.value)
    IO.write(file, contents)
    Seq(file)
  }

  val rawlsCompileSettings = List(
    Compile / resourceGenerators += writeVersionConf
  )
}
