package org.broadinstitute.dsde.rawls

import akka.http.scaladsl.model.StatusCode
import org.broadinstitute.dsde.rawls.model.{ErrorReport, ErrorReportSource}

class RawlsException(message: String = null, cause: Throwable = null) extends Exception(message, cause)

class RawlsExceptionWithErrorReport(val errorReport: ErrorReport) extends RawlsException(errorReport.toString)

object RawlsExceptionWithErrorReport {
  def apply(errorReport: ErrorReport): RawlsExceptionWithErrorReport = new RawlsExceptionWithErrorReport(errorReport)

  def apply(message: String)(implicit source: ErrorReportSource): RawlsExceptionWithErrorReport =
    RawlsExceptionWithErrorReport(ErrorReport(message))

  def apply(message: String, cause: ErrorReport)(implicit source: ErrorReportSource): RawlsExceptionWithErrorReport =
    RawlsExceptionWithErrorReport(ErrorReport(message, cause))

  def apply(status: StatusCode, message: String)(implicit source: ErrorReportSource): RawlsExceptionWithErrorReport =
    RawlsExceptionWithErrorReport(ErrorReport(status, message))
}

/**
  * An exception where retrying will not help.
  *
  * @param errorReport The report to the user on what went wrong.
  */
class RawlsFatalExceptionWithErrorReport(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)
