package org.broadinstitute.dsde.rawls.submissions

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport, StringValidationUtils}
import org.broadinstitute.dsde.rawls.model.WorkflowFailureModes.WorkflowFailureMode
import org.broadinstitute.dsde.rawls.model.{ErrorReport, ErrorReportSource, MethodConfiguration, SubmissionRequest, WorkflowFailureModes}

import scala.util.{Failure, Success, Try}

object SubmissionValidation extends StringValidationUtils {

  implicit val errorReportSource: ErrorReportSource = ErrorReportSource("rawls")

  val COST_CAP_THRESHOLD_SCALE = 2;
  val COST_CAP_THRESHOLD_PRECISION = 10;

  // Note: this limit is also hard-coded in the terra-ui code to allow client-side validation.
  // If it is changed, it must also be updated in that repository.
  val UserCommentMaxLength: Int = 1000

  /**
    * Preform validation of a submission request that can be done with no external dependencies
    */
  def staticValidation(submission: SubmissionRequest,
                       methodConfig: MethodConfiguration): Unit = {

    val errors = List(
      validateCostCapThreshold(submission),
      validateEntityNameAndType(submission),
      validateMethodConfigRootEntity(submission, methodConfig),
      validateEntityAndDataReference(submission, methodConfig),
      submission.userComment.flatMap(validateMaxStringLengthWithReport(_, "userComment", UserCommentMaxLength)),
      validateWorkflowFailureMode(submission),
    ).flatten
    if (errors.nonEmpty)
      throw new RawlsExceptionWithErrorReport(
        ErrorReport(StatusCodes.BadRequest, "Failed submission validations", errors)
      )
  }


    def validateCostCapThreshold(submissionRequest: SubmissionRequest): List[ErrorReport] = List(
        submissionRequest.costCapThreshold.flatMap { threshold =>
          if (threshold.sign <= 0) Some(ErrorReport("costCapThreshold must be greater than zero")) else None
        },
        submissionRequest.costCapThreshold.flatMap { threshold =>
          if (threshold.scale > COST_CAP_THRESHOLD_SCALE) {
            // TODO: improve messages
            Some(ErrorReport(s"costCapThreshold scale cannot be greater than $COST_CAP_THRESHOLD_SCALE"))
          } else None
        },
        submissionRequest.costCapThreshold.flatMap { threshold =>
          if (threshold.precision > COST_CAP_THRESHOLD_PRECISION)
            Some(ErrorReport(s"costCapThreshold cannot be greater than $COST_CAP_THRESHOLD_PRECISION total digits"))
          else None
        }
      ).flatten


  def validateEntityNameAndType(submission: SubmissionRequest): Option[ErrorReport] = {
    if (submission.entityName.isDefined != submission.entityType.isDefined) Some(ErrorReport(
      StatusCodes.BadRequest,
        s"You must set both entityType and entityName to run on an entity, or neither (to run with literal or workspace inputs)."
    )) else None
  }


  def validateMethodConfigRootEntity(submission: SubmissionRequest, methodConfig: MethodConfiguration): Option[ErrorReport] = {
    (methodConfig.dataReferenceName, methodConfig.rootEntityType, submission.entityName) match {
      case (Some(_), _, _) => None
      case (None, Some(_), Some(_)) => None
      case (None, None, None) => None
      case (None, Some(_), None) =>
        Some(ErrorReport(StatusCodes.BadRequest, s"Your method config defines a root entity but you haven't passed one to the submission."))
      // This isn't _strictly_ necessary, since a single submission entity will create one workflow.
      // However, passing in a submission entity + an expression doesn't make sense for two reasons:
      // 1. you'd have to write an expression from your submission entity to an entity of "no entity necessary" type
      // 2. even if you _could_ do this, you'd kick off a bunch of identical workflows.
      // More likely than not, an MC with no root entity + a submission entity = you're doing something wrong. So we'll just say no here.
      case (None, None, Some(_)) =>
        Some(ErrorReport(StatusCodes.BadRequest, s"Your method config uses no root entity, but you passed one to the submission."))
    }
  }

  def validateEntityAndDataReference(submission: SubmissionRequest, methodConfig: MethodConfiguration): Option[ErrorReport] = {
    if( methodConfig.dataReferenceName.isDefined && submission.entityName.isDefined )
      Some(ErrorReport(
        StatusCodes.BadRequest,
        "Your method config defines a data reference and an entity name. Running on a submission on a single entity in a data reference is not yet supported."
      )
    ) else None
  }


  def validateWorkflowFailureMode(submissionRequest: SubmissionRequest): Option[ErrorReport] =
    Try(submissionRequest.workflowFailureMode.map(WorkflowFailureModes.withName)) match {
      case Success(_) => None
      case Failure(e: RawlsException) => Some(ErrorReport(StatusCodes.BadRequest, e.getMessage))
      case Failure(e) =>
        throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.InternalServerError, e.getMessage))
    }

}
