package org.broadinstitute.dsde.rawls.model

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import spray.http.StatusCodes
import spray.httpx.UnsuccessfulResponseException

trait ErrorReportable {
  def errorReportSource: ErrorReportSource

  def toErrorReport(throwable: Throwable) = {
    throwable match {
      case gjre: GoogleJsonResponseException =>
        val statusCode = StatusCodes.getForKey(gjre.getStatusCode)
        ErrorReport(ErrorReport.message(gjre), statusCode, ErrorReport.causes(gjre)(errorReportSource), Seq.empty, Option(gjre.getClass))(errorReportSource)
      case ure: UnsuccessfulResponseException =>
        ErrorReport(ErrorReport.message(ure), Option(ure.response.status), ErrorReport.causes(throwable)(errorReportSource), Seq.empty, Option(ure.getClass))(errorReportSource)
      case _ =>
        ErrorReport(ErrorReport.message(throwable), None, ErrorReport.causes(throwable)(errorReportSource), throwable.getStackTrace, Option(throwable.getClass))(errorReportSource)
    }
  }
}
