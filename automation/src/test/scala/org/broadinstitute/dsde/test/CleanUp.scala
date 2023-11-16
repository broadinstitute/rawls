package org.broadinstitute.dsde.test

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.model.ErrorReportJsonSupport._
import org.broadinstitute.dsde.workbench.model.{ErrorReport, ErrorReportSource, WorkbenchExceptionWithErrorReport}
import org.broadinstitute.dsde.workbench.service.util.ExceptionHandling
import org.scalatest.{Outcome, TestSuite, TestSuiteMixin}
import spray.json._
import org.broadinstitute.dsde.workbench.service.test.{CleanUp => WBCleanup}

import java.util.concurrent.ConcurrentLinkedDeque
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/**
  * Mix-in for cleaning up data created during a test.
  */
trait CleanUp extends TestSuiteMixin with ExceptionHandling with LazyLogging with WBCleanup { self: TestSuite =>
  override implicit val errorReportSource: ErrorReportSource = ErrorReportSource(self.suiteName)

  private val cleanUpFunctions = new ConcurrentLinkedDeque[() => Any]()



  /**
    * Function for controlling when the clean-up functions will be run.
    * Clean-up functions are usually run at the end of the test, which includes
    * clean-up from loan-fixture methods. This can cause problems if there are
    * dependencies, such as foreign key references, between test data created
    * in a loan-fixture method and the test itself:
    *
    * <pre>
    * "tries to clean-up parent before child" in {
    *   withParent { parent =>  // withParent loan-fixture clean-up will run before registered clean-up functions
    *     child = Child(parent)
    *     register cleanUp { delete child }
    *     ...
    *   }
    * }
    * </pre>
    *
    * Use withCleanUp to explicitly control when the registered clean-up
    * methods will be called:
    *
    * <pre>
    * "clean-up child before parent" in {
    *   withParent { parent =>
    *     withCleanUp {  // registered clean-up functions will run before enclosing loan-fixture clean-up
    *       child = Child(parent)
    *       register cleanUp { delete child }
    *       ...
    *     }
    *   }
    * }
    * </pre>
    *
    * Note that this is not needed if the dependent objects are contributed by
    * separate loan-fixture methods whose execution order can be explicitly
    * controlled:
    *
    * <pre>
    * "clean-up inner loan-fixtures first" in {
    *   withParent { parent =>
    *     withChild(parent) { child =>
    *       ...
    *     }
    *   }
    * }
    *
    * @param testCode the test code to run
    */
  override def withCleanUp(testCode: => Any): Unit = {
    val testTrial = Try(testCode)
    val cleanupTrial = Try(runCleanUpFunctions())
    runCodeWithCleanup(testTrial, cleanupTrial)
  }

  abstract override def withFixture(test: NoArgTest): Outcome = {
    if (cleanUpFunctions.peek() != null) throw new Exception("cleanUpFunctions non empty at start of withFixture block")
    val testTrial = Try(super.withFixture(test))
    val cleanupTrial = Try(runCleanUpFunctions())
    runCodeWithCleanup(testTrial, cleanupTrial)
  }

  private def runCleanUpFunctions(): Unit = {

  }

  def runCodeWithCleanup[T, C](testTrial: Try[T], cleanupTrial: Try[C]): T =
    (testTrial, cleanupTrial) match {
      case (Success(outcome), Success(_)) => outcome
      case (Failure(t), Success(_)) => throw t
      case (Success(outcome), Failure(t)) =>
        val report = ErrorReport(s"Test passed but cleanup failed: ${t.getMessage}", ErrorReport(t))
        logger.error(report.message)
        outcome
      case (Failure(t), Failure(c)) =>
        throw new WorkbenchExceptionWithErrorReport(
          ErrorReport(
            "Test and CleanUp both failed. This ErrorReport's causes contain the test and cleanup exceptions, in that order",
            Seq(ErrorReport(t), ErrorReport(c))
          )
        )
    }
}

object CleanUp {
  implicit val errorReportSource: ErrorReportSource = ErrorReportSource("WorkbenchLibs.CleanUp")


}
