import sbt.Keys._
import sbt._

object Testing {
  lazy val validMySqlHost = taskKey[Unit]("Determine if mysql.host is provided.")

  lazy val validMySqlHostSetting = validMySqlHost := Def.taskDyn {
    val hostName = sys.props.get("mysql.host")
    val log = streams.value.log

    Def.task {
      if (hostName.isEmpty) {
        log.error(
          "Database host name not set. Please see run instructions in README.md for providing a valid database hostname"
        )
        sys.exit(1)
      }
    }
  }.value

  def isIntegrationTest(name: String): Boolean = name contains "integrationtest"

  lazy val IntegrationTest = config("it") extend Test

  val testSettingsWithoutDb: Seq[Setting[_]] = List(
    Test / testOptions += Tests.Setup(() => sys.props += "mockserver.logLevel" -> "WARN"),
    Test / testOptions ++= Seq(Tests.Filter(s => !isIntegrationTest(s))),
    Test / testOptions += Tests.Argument("-oD"), // D = individual test durations
    IntegrationTest / testOptions := Seq(Tests.Filter(s => isIntegrationTest(s))),
    Test / parallelExecution := false
  )

  val testSettingsWithDb: Seq[Setting[_]] = List(
    validMySqlHostSetting,
    (Test / test) := ((Test / test) dependsOn validMySqlHost).value,
    (Test / testOnly) := ((Test / testOnly) dependsOn validMySqlHost).evaluated
  ) ++ (if (sys.props.getOrElse("secrets.skip", "false") != "true") MinnieKenny.testSettings else List())

  implicit class ProjectTestSettings(val project: Project) extends AnyVal {
    def withTestSettings: Project = project
      .configs(IntegrationTest)
      .settings(inConfig(IntegrationTest)(Defaults.testTasks): _*)
  }
}
