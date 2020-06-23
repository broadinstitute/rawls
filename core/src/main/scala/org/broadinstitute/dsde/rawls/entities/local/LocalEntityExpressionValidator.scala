package org.broadinstitute.dsde.rawls.entities.local

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.entities.base.ExpressionValidator
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.GatherInputsResult
import org.broadinstitute.dsde.rawls.model.{AttributeString, ErrorReport, MethodConfiguration, ValidatedMethodConfiguration}
import slick.dbio.DBIOAction

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class LocalEntityExpressionValidator(dataSource: SlickDataSource)(implicit protected val executionContext: ExecutionContext) extends ExpressionValidator {
  // validate a MC, skipping optional empty inputs, and return a ValidatedMethodConfiguration
  def validateAndParseMCExpressions(methodConfiguration: MethodConfiguration,
                                    gatherInputsResult: GatherInputsResult,
                                    allowRootEntity: Boolean): Future[ValidatedMethodConfiguration] = {

    dataSource.inTransaction { parser =>
      val inputsToParse = gatherInputsResult.processableInputs map { mi => (mi.workflowInput.getName, AttributeString(mi.expression)) }
      val (emptyOutputs, outputsToParse) = methodConfiguration.outputs.partition { case (_, expr) => expr.value.isEmpty }

      val parsed = parser.parseMCExpressions(
        inputs = inputsToParse.toMap,
        outputs = outputsToParse,
        allowRootEntity = allowRootEntity,
        rootEntityTypeOption = methodConfiguration.rootEntityType
      )

      // empty output expressions are also valid
      val validatedOutputs = emptyOutputs.keys.toSet ++ parsed.validOutputs

      // a MethodInput which is both optional and empty is already valid
      val emptyOptionalInputs = gatherInputsResult.emptyOptionalInputs map {
        _.workflowInput.getName
      }

      DBIOAction.successful(ValidatedMethodConfiguration(methodConfiguration, parsed.validInputs ++ emptyOptionalInputs, parsed.invalidInputs, gatherInputsResult.missingInputs, gatherInputsResult.extraInputs, validatedOutputs, parsed.invalidOutputs))
    }
  }

  // validate a MC, skipping optional empty inputs, and return failure when any inputs/outputs are invalid
  def validateExpressionsForSubmission(methodConfiguration: MethodConfiguration,
                                       gatherInputsResult: GatherInputsResult,
                                       allowRootEntity: Boolean): Future[Try[ValidatedMethodConfiguration]] = {

    validateAndParseMCExpressions(methodConfiguration, gatherInputsResult, allowRootEntity).map { validated =>

      Try {
        if (validated.invalidInputs.nonEmpty || validated.missingInputs.nonEmpty || validated.extraInputs.nonEmpty || validated.invalidOutputs.nonEmpty) {
          val inputMsg = if (validated.invalidInputs.isEmpty) Seq() else Seq(s"Invalid inputs: ${validated.invalidInputs.mkString(",")}")
          val missingMsg = if (validated.missingInputs.isEmpty) Seq() else Seq(s"Missing inputs: ${validated.missingInputs.mkString(",")}")
          val extrasMsg = if (validated.extraInputs.isEmpty) Seq() else Seq(s"Extra inputs: ${validated.extraInputs.mkString(",")}")
          val outputMsg = if (validated.invalidOutputs.isEmpty) Seq() else Seq(s"Invalid outputs: ${validated.invalidOutputs.mkString(",")}")
          val errorStr = (inputMsg ++ missingMsg ++ extrasMsg ++ outputMsg) mkString " ; "
          throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"Validation errors: $errorStr"))
        }
        validated
      }
    }
  }
}
