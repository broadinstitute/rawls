package org.broadinstitute.dsde.rawls

import akka.http.scaladsl.model.StatusCodes
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.broadinstitute.dsde.rawls.dataaccess.DsdeHttpDAO
import org.broadinstitute.dsde.rawls.model.ErrorReport

class DsdeHttpDAOSpec extends AnyFlatSpecLike with Matchers {
  behavior of "when5xx"

  val matches5xxAnswers = Map(
    StatusCodes.InternalServerError -> true, // 500
    StatusCodes.ServiceUnavailable -> true, // 503
    StatusCodes.BadRequest -> false, // 400
    StatusCodes.Unauthorized -> false, // 400
    StatusCodes.OK -> false // 200
  )

  matches5xxAnswers foreach { case (code, answer) =>
    val assertionMessage = s"${if (answer) "" else "not "}match status code ${code.reason}"
    it should assertionMessage in {
      val e = new RawlsExceptionWithErrorReport(ErrorReport(code, ""))
      DsdeHttpDAO.when5xx(e) should be(answer)
    }
  }
}
