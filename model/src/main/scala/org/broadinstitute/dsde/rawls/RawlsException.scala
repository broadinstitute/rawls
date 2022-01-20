package org.broadinstitute.dsde.rawls

import org.broadinstitute.dsde.rawls.model.ErrorReport

class RawlsException(message: String = null, cause: Throwable = null) extends Exception(message, cause)

class RawlsExceptionWithErrorReport(val errorReport: ErrorReport) extends RawlsException(errorReport.toString)

/**
  * An exception where retrying will not help.
  *
  * @param errorReport The report to the user on what went wrong.
  */
class RawlsFatalExceptionWithErrorReport(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)
