import sbt.Keys._
import sbt._

object Testing {
  lazy val validMySqlHost = taskKey[Unit]("Determine if mysql.host is provided.")

  lazy val validMySqlHostSetting = validMySqlHost := Def.taskDyn {
    val hostName = sys.props.getOrElse("mysql.host", "")
    if (hostName.length == 0) {
      streams.map(x => x.log.error("Database host name not set. Please see run instructions in README.md for providing a valid database hostname")).value
    }
    sys.exit()
  }.value

<<<<<<< e24ae619300c7eba1bcf576f13820e24f7cc5639
  lazy val validDirectoryUrl = taskKey[Unit]("Determine if directory.url is provided.")
  lazy val validDirectoryPassword = taskKey[Unit]("Determine if directory.password is provided.")

  lazy val validDirectoryUrlSetting = validDirectoryUrl := Def.taskDyn {
    val setting = sys.props.getOrElse("directory.url", "")
    if (setting.length == 0) {
      streams.map(x => x.log.error("directory.url not set")).value
    }
    sys.exit()
  }.value

  lazy val validDirectoryPasswordSetting = validDirectoryPassword := Def.taskDyn {
    val setting = sys.props.getOrElse("directory.password", "")
    if (setting.length == 0) {
      streams.map(x => x.log.error("directory.password not set")).value
    }
    sys.exit()
  }.value

=======
>>>>>>> move workspace access control to sam
  def isIntegrationTest(name: String) = name contains "integrationtest"

  lazy val IntegrationTest = config("it") extend Test

  val commonTestSettings: Seq[Setting[_]] = List(

    // SLF4J initializes itself upon the first logging call.  Because sbt
    // runs tests in parallel it is likely that a second thread will
    // invoke a second logging call before SLF4J has completed
    // initialization from the first thread's logging call, leading to
    // these messages:
    //   SLF4J: The following loggers will not work because they were created
    //   SLF4J: during the default configuration phase of the underlying logging system.
    //   SLF4J: See also http://www.slf4j.org/codes.html#substituteLogger
    //   SLF4J: com.imageworks.common.concurrent.SingleThreadInfiniteLoopRunner
    //
    // As a workaround, load SLF4J's root logger before starting the unit
    // tests

    // Source: https://github.com/typesafehub/scalalogging/issues/23#issuecomment-17359537
    // References:
    //   http://stackoverflow.com/a/12095245
    //   http://jira.qos.ch/browse/SLF4J-167
    //   http://jira.qos.ch/browse/SLF4J-97
    testOptions in Test += Tests.Setup(classLoader =>
      classLoader
        .loadClass("org.slf4j.LoggerFactory")
        .getMethod("getLogger", classLoader.loadClass("java.lang.String"))
        .invoke(null, "ROOT")
    ),
    testOptions in Test ++= Seq(Tests.Filter(s => !isIntegrationTest(s))),
    testOptions in IntegrationTest := Seq(Tests.Filter(s => isIntegrationTest(s))),

    validMySqlHostSetting,

<<<<<<< e24ae619300c7eba1bcf576f13820e24f7cc5639
    (test in Test) := ((test in Test) dependsOn(validDirectoryUrl, validDirectoryPassword)).value,
    (testOnly in Test) := ((testOnly in Test) dependsOn(validDirectoryUrl, validDirectoryPassword)).inputTaskValue.evaluated,
    (test in Test) := ((test in Test) dependsOn validMySqlHost).value,
    (testOnly in Test) := ((testOnly in Test) dependsOn validMySqlHost).inputTaskValue.evaluated,
=======
    (test in Test) <<= (test in Test) dependsOn validMySqlHost,
    (testOnly in Test) <<= (testOnly in Test) dependsOn validMySqlHost,
>>>>>>> move workspace access control to sam

    parallelExecution in Test := false


  )

  implicit class ProjectTestSettings(val project: Project) extends AnyVal {
    def withTestSettings: Project = project
      .configs(IntegrationTest).settings(inConfig(IntegrationTest)(Defaults.testTasks): _*)
  }
}

