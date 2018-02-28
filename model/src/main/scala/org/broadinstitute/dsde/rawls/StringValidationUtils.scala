package org.broadinstitute.dsde.rawls

import org.broadinstitute.dsde.rawls.model.{Attributable, AttributeName, ErrorReport, ErrorReportSource}
import akka.http.scaladsl.model.StatusCodes

trait StringValidationUtils {
  implicit val errorReportSource: ErrorReportSource

  //in general, we only support alphanumeric, spaces, _, and - for user-input
  private lazy val userDefinedRegex = "[A-z0-9_-]+".r
  def validateUserDefinedString(s: String): Unit = {
    if(! userDefinedRegex.pattern.matcher(s).matches) {
      val msg = s"Invalid input: $s. Input may only contain alphanumeric characters, underscores, and dashes."
      throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(message = msg, statusCode = StatusCodes.BadRequest))
    }
  }

  def validateMaxStringLength(s: String, maxLength: Int): Unit = {
    if(s.length > maxLength) throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(message = s"Invalid input: $s. Input may be a max of $maxLength characters.", statusCode = StatusCodes.BadRequest))
  }

  def validateAttributeName(an: AttributeName, entityType: String): Unit = {
    if (Attributable.reservedAttributeNames.exists(_.equalsIgnoreCase(an.name)) ||
      AttributeName.withDefaultNS(entityType + Attributable.entityIdAttributeSuffix).equalsIgnoreCase(an)) {

      throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(message = s"Attribute name ${an.name} is reserved", statusCode = StatusCodes.BadRequest))
    }
  }
}
