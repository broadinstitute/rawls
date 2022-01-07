package org.broadinstitute.dsde.rawls

import akka.http.scaladsl.model.StatusCodes
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.broadinstitute.dsde.rawls.dataaccess.DsdeHttpDAO
import org.broadinstitute.dsde.rawls.model.ErrorReport

class DsdeHttpDAOSpec extends AnyFlatSpecLike with Matchers {
  behavior of "when5xx"

  it should "match 500" in {
    val e = new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.InternalServerError, ""))
    DsdeHttpDAO.when5xx(e) should be(true)
  }

  it should "match 503" in {
    val e = new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.ServiceUnavailable, ""))
    DsdeHttpDAO.when5xx(e) should be(true)
  }
}
