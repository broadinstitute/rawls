package org.broadinstitute.dsde.rawls.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Created by dvoet on 2/24/17.
 */
class ErrorReportableSpec extends AnyFlatSpec with Matchers {

  "ErrorReportable" should "have the right source for causes" in {
    implicit val outerErrorReportSource = ErrorReportSource("foo")

    val cause = new Exception("exception", new Exception("cause"))

    val errorReportable = new ErrorReportable {
      override def errorReportSource: ErrorReportSource = ErrorReportSource("bar")
    }

    val report = ErrorReport("message", errorReportable.toErrorReport(cause))

    assertResult("foo")(report.source)
    assertResult("bar")(report.causes.head.source)
    assertResult("bar")(report.causes.head.causes.head.source)
  }
}
