package org.broadinstitute.dsde.rawls

import org.broadinstitute.dsde.rawls.model.ErrorReport
import spray.http.{StatusCodes, StatusCode}

class RawlsException(message: String = null, cause: Throwable = null) extends Exception(message, cause)

class RawlsExceptionWithErrorReport(val errorReport: ErrorReport) extends RawlsException(errorReport.toString)