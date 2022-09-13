import sbt.Keys._
import sbt._
import complete.DefaultParsers._
import sbt.util.Logger

import scala.sys.process._

object MinnieKenny {
  val minnieKenny = inputKey[Unit]("Run minnie-kenny.")

  /** Run minnie-kenny only once per sbt invocation. */
  class MinnieKennySingleRunner() {
    private val mutex = new Object
    private var resultOption: Option[Int] = None

    /** Run using the logger, throwing an exception only on the first failure. */
    def runOnce(log: Logger, args: Seq[String]): Unit = {
      mutex synchronized {
        if (resultOption.isEmpty) {
          log.debug(s"Running minnie-kenny.sh${args.mkString(" ", " ", "")}")
          val result = ("./minnie-kenny.sh" +: args) ! log
          resultOption = Option(result)
          if (result == 0)
            log.debug("Successfully ran minnie-kenny.sh")
          else
            sys.error("Running minnie-kenny.sh failed. Please double check for errors above.")
        }
      }
    }
  }

  // Only run one minnie-kenny.sh at a time!
  private lazy val minnieKennySingleRunner = new MinnieKennySingleRunner

  val testSettings = List(
    minnieKenny := {
      val log = streams.value.log
      val args = spaceDelimited("<arg>").parsed
      minnieKennySingleRunner.runOnce(log, args)
    },
    Test / test := {
      minnieKenny.toTask("").value
      (Test / test).value
    }
  )
}
