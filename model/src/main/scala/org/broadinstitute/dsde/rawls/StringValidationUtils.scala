package org.broadinstitute.dsde.rawls

import org.broadinstitute.dsde.rawls.model.{Attributable, AttributeName, ErrorReport, ErrorReportSource}
import akka.http.scaladsl.model.StatusCodes

import scala.concurrent.Future

trait StringValidationUtils {
  implicit val errorReportSource: ErrorReportSource

  private lazy val userDefinedRegex = "[A-z0-9_-]+".r
  def validateUserDefinedString(s: String): Unit = {
    if(! userDefinedRegex.pattern.matcher(s).matches) {
      val msg = s"Invalid input: $s. Input may only contain alphanumeric characters, underscores, and dashes."
      throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(message = msg, statusCode = StatusCodes.BadRequest))
    }
  }

  private lazy val entityNameRegex = "[A-z0-9\\._-]+".r
  def validateEntityName(s: String): Unit = {
    if(! entityNameRegex.pattern.matcher(s).matches) {
      val msg = s"Invalid input: $s. Input may only contain alphanumeric characters, underscores, dashes, and periods."
      throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(message = msg, statusCode = StatusCodes.BadRequest))
    }
  }

  private lazy val workspaceNameRegex = "[A-z0-9 _-]+".r
  def validateWorkspaceName(s: String): Unit = {
    if(! workspaceNameRegex.pattern.matcher(s).matches) {
      val msg = s"Invalid input: $s. Input may only contain alphanumeric characters, underscores, dashes, and spaces."
      throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(message = msg, statusCode = StatusCodes.BadRequest))
    }
  }

  private lazy val billingProjectNameRegex = "[A-z0-9_-]{6,30}".r
  def validateBillingProjectName(s: String): Future[Unit] = {
    if(! billingProjectNameRegex.pattern.matcher(s).matches) {
      val msg = s"Invalid name for billing project. Input must be between 6 and 30 characters in length and may only contain alphanumeric characters, underscores, and dashes."
      Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(message = msg, statusCode = StatusCodes.BadRequest)))
    } else {
      Future.successful(())
    }
  }

  private lazy val billingAccountNameRegex = "[A-z0-9]{6}-[A-z0-9]{6}-[A-z0-9]{6}$".r
  def validateBillingAccountName(s: String): Unit = {
    if(! billingAccountNameRegex.pattern.matcher(s).matches) {
      val msg = s"Invalid input: $s. Input must be of the format XXXXXX-XXXXXX-XXXXXX."
      throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(message = msg, statusCode = StatusCodes.BadRequest))
    }
  }

  //See https://cloud.google.com/bigquery/docs/datasets#dataset-naming
  private lazy val bigQueryDatasetNameRegex = "[A-z0-9_]{1,1024}".r
  def validateBigQueryDatasetName(s: String): Unit = {
    if(! bigQueryDatasetNameRegex.pattern.matcher(s).matches) {
      val msg = s"Invalid name for dataset. Input must be between 1 and 1024 characters in length and may only contain alphanumeric characters and underscores."
      throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(message = msg, statusCode = StatusCodes.BadRequest))
    }
  }

  def validateMaxStringLength(s: String, maxLength: Int): Unit = {
    if(s.length > maxLength) throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(message = s"Invalid input: $s. Input may be a max of $maxLength characters.", statusCode = StatusCodes.BadRequest))
  }

  def validateAttributeName(an: AttributeName, entityType: String): Unit = {
    if (Attributable.reservedAttributeNames.exists(_.equalsIgnoreCase(an.name)) ||
      AttributeName.withDefaultNS(entityType + Attributable.entityIdAttributeSuffix).equalsIgnoreCase(an)) {

      throw new RawlsFatalExceptionWithErrorReport(errorReport = ErrorReport(
        message = s"Attribute name ${an.name} is reserved and cannot be overwritten",
        statusCode = StatusCodes.BadRequest
      ))
    }
  }
}
