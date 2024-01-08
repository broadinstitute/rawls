package org.broadinstitute.dsde.rawls

import org.broadinstitute.dsde.rawls.model.ErrorReportSource

package object google {
  implicit val errorReportSource: ErrorReportSource = ErrorReportSource("google")
}
