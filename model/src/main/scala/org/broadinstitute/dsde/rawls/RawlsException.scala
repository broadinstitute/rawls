package org.broadinstitute.dsde.rawls

import org.broadinstitute.dsde.rawls.model.CErrorReport

class RawlsException(message: String = null, cause: Throwable = null) extends Exception(message, cause)

class RawlsExceptionWithErrorReport(val errorReport: CErrorReport) extends RawlsException(errorReport.toString)