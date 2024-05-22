package org.broadinstitute.dsde.rawls

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.model.{
  Attributable,
  AttributeName,
  ErrorReport,
  ErrorReportSource,
  GoogleProjectId
}
import org.broadinstitute.dsde.workbench.model.google.BigQueryDatasetName

import scala.concurrent.Future

trait StringValidationUtils {
  implicit val errorReportSource: ErrorReportSource

  private lazy val userDefinedRegex = "[A-z0-9_-]+".r
  def validateUserDefinedString(s: String): Unit =
    if (!userDefinedRegex.pattern.matcher(s).matches) {
      val msg = s"Invalid input: $s. Input may only contain alphanumeric characters, underscores, and dashes."
      throw new RawlsExceptionWithErrorReport(
        errorReport = ErrorReport(message = msg, statusCode = StatusCodes.BadRequest)
      )
    }

  private lazy val entityNameRegex = "[A-z0-9\\._-]+".r
  def validateEntityName(s: String): Unit =
    if (!entityNameRegex.pattern.matcher(s).matches) {
      val msg =
        s"""Invalid entity name: $s. Input may only contain alphanumeric characters, underscores, dashes, and periods. 
            If importing a snapshot, making an API call, uploading a TSV, or adding a new row from the Terra UI, 
            please modify the impacted name(s) and try again.""".stripMargin
      throw new RawlsExceptionWithErrorReport(
        errorReport = ErrorReport(message = msg, statusCode = StatusCodes.BadRequest)
      )
    }

  private lazy val workspaceNameRegex = "[A-z0-9 _-]+".r
  def validateWorkspaceName(s: String): Unit =
    if (!workspaceNameRegex.pattern.matcher(s).matches) {
      val msg = s"Invalid input: $s. Input may only contain alphanumeric characters, underscores, dashes, and spaces."
      throw new RawlsExceptionWithErrorReport(
        errorReport = ErrorReport(message = msg, statusCode = StatusCodes.BadRequest)
      )
    }

  private lazy val billingAccountNameRegex = "billingAccounts/[A-z0-9]{6}-[A-z0-9]{6}-[A-z0-9]{6}$".r
  def validateBillingAccountName(s: String): Unit =
    if (!billingAccountNameRegex.pattern.matcher(s).matches) {
      val msg = s"Invalid input: $s. Input must be of the format billingAccounts/XXXXXX-XXXXXX-XXXXXX."
      throw new RawlsExceptionWithErrorReport(
        errorReport = ErrorReport(message = msg, statusCode = StatusCodes.BadRequest)
      )
    }

  private lazy val billingProjectNameRegex = "[A-z0-9_-]{6,30}".r
  def validateBillingProjectName(s: String): Future[Unit] =
    if (!billingProjectNameRegex.pattern.matcher(s).matches) {
      val msg =
        s"Invalid name for billing project. Input must be between 6 and 30 characters in length and may only contain alphanumeric characters, underscores, and dashes."
      Future.failed(
        new RawlsExceptionWithErrorReport(errorReport = ErrorReport(message = msg, statusCode = StatusCodes.BadRequest))
      )
    } else {
      Future.successful(())
    }

  // See Google docs for naming requirements: https://cloud.google.com/resource-manager/docs/creating-managing-projects#before_you_begin
  private lazy val googleProjectNameRegex = "^[a-z]([-a-z0-9]){4,28}[a-z0-9]$".r
  def validateGoogleProjectName(s: String): Future[Unit] =
    if (!googleProjectNameRegex.pattern.matcher(s).matches) {
      val msg =
        s"Invalid name for Google project. Input must be 6 to 30 lowercase letters, digits, or hyphens. It must start with a letter. Trailing hyphens are prohibited."
      Future.failed(
        new RawlsExceptionWithErrorReport(errorReport = ErrorReport(message = msg, statusCode = StatusCodes.BadRequest))
      )
    } else {
      Future.successful(())
    }

  // See https://cloud.google.com/bigquery/docs/datasets#dataset-naming
  private lazy val bigQueryDatasetNameRegex = "[A-z0-9_]{1,1024}".r
  def validateBigQueryDatasetName(s: BigQueryDatasetName): Unit =
    if (!bigQueryDatasetNameRegex.pattern.matcher(s.value).matches) {
      val msg =
        s"Invalid name for dataset. Input must be between 1 and 1024 characters in length and may only contain alphanumeric characters and underscores."
      throw new RawlsExceptionWithErrorReport(
        errorReport = ErrorReport(message = msg, statusCode = StatusCodes.BadRequest)
      )
    }

  def validateMaxStringLength(str: String, inputName: String, maxLength: Int): Unit =
    if (str.length > maxLength)
      throw new RawlsExceptionWithErrorReport(
        errorReport = ErrorReport(message = s"Invalid input $inputName. Input may be a max of $maxLength characters.",
                                  statusCode = StatusCodes.BadRequest
        )
      )

  def validateAttributeName(an: AttributeName, entityType: String): Unit =
    if (
      Attributable.reservedAttributeNames.exists(_.equalsIgnoreCase(an)) ||
      AttributeName.withDefaultNS(entityType + Attributable.entityIdAttributeSuffix).equalsIgnoreCase(an)
    ) {

      throw new RawlsFatalExceptionWithErrorReport(
        errorReport = ErrorReport(
          message = s"Attribute name ${an.name} is reserved and cannot be overwritten",
          statusCode = StatusCodes.BadRequest
        )
      )
    }
}
