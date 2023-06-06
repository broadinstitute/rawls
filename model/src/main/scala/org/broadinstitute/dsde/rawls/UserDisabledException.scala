package org.broadinstitute.dsde.rawls

import akka.http.scaladsl.model.StatusCode
import org.broadinstitute.dsde.rawls.model.{ErrorReport, ErrorReportSource}

class UserDisabledException(statusCode: StatusCode, message: String)(implicit source: ErrorReportSource)
    extends RawlsExceptionWithErrorReport(ErrorReport(statusCode, message))
