package org.broadinstitute.dsde.rawls.dataaccess

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import org.broadinstitute.dsde.rawls.model.ErrorReport
import spray.http.StatusCodes
import spray.httpx.UnsuccessfulResponseException

trait ErrorReportable {
  val errorReportSource: String

  def toErrorReport(throwable: Throwable) = {
    throwable match {
      case gjre: GoogleJsonResponseException =>
        val statusCode = StatusCodes.getForKey(gjre.getStatusCode)
        ErrorReport(errorReportSource, ErrorReport.message(gjre), statusCode, ErrorReport.causes(gjre), Seq.empty)
      case ure: UnsuccessfulResponseException =>
        ErrorReport(errorReportSource, ErrorReport.message(ure), Option(ure.response.status), ErrorReport.causes(throwable), Seq.empty)
      case _ =>
        ErrorReport(errorReportSource, ErrorReport.message(throwable), None, ErrorReport.causes(throwable), throwable.getStackTrace)
    }
  }

}
