package org.broadinstitute.dsde.rawls.expressions

import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.model.{AttributeString, ErrorReport, MethodConfiguration, ValidatedMethodConfiguration}
import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.GatherInputsResult

import scala.util.Try

object ExpressionValidator {
  // inputsToParse is parameterized in order to validate in the presence or absence of the associated Method
  // presence: inputs which are both empty and optional are pre-validated, so they are skipped here
  // absence: validate all inputs normally

  /*
  private[expressions] def validateAndParse(methodConfiguration: MethodConfiguration, gatherInputsResult: GatherInputsResult, allowRootEntity: Boolean, parser: SlickExpressionParser): ValidatedMethodConfiguration = {
    val inputsToParse = gatherInputsResult.processableInputs map { mi => (mi.workflowInput.localName.value, AttributeString(mi.expression)) }
    val (emptyOutputs, outputsToParse) = methodConfiguration.outputs.partition { case (_, expr) => expr.value.isEmpty }

    val parsed = ExpressionParser.parseMCExpressions(inputsToParse.toMap, outputsToParse, allowRootEntity, parser)

    // empty output expressions are also valid
    val validatedOutputs = emptyOutputs.keys.toSet ++ parsed.validOutputs

    ValidatedMethodConfiguration(methodConfiguration, parsed.validInputs, parsed.invalidInputs, gatherInputsResult.missingInputs, gatherInputsResult.extraInputs, validatedOutputs, parsed.invalidOutputs)
  }
  */

  // validate a MC, skipping optional empty inputs, and return a ValidatedMethodConfiguration
  def validateAndParseMCExpressions(methodConfiguration: MethodConfiguration,
                                    gatherInputsResult: GatherInputsResult,
                                    allowRootEntity: Boolean,
                                    parser: SlickExpressionParser): ValidatedMethodConfiguration = {

    val inputsToParse = gatherInputsResult.processableInputs map { mi => (mi.workflowInput.getName, AttributeString(mi.expression)) }
    val (emptyOutputs, outputsToParse) = methodConfiguration.outputs.partition { case (_, expr) => expr.value.isEmpty }

    val parsed = ExpressionParser.parseMCExpressions(inputsToParse.toMap, outputsToParse, allowRootEntity, parser)

    // empty output expressions are also valid
    val validatedOutputs = emptyOutputs.keys.toSet ++ parsed.validOutputs

    // a MethodInput which is both optional and empty is already valid
    val emptyOptionalInputs = gatherInputsResult.emptyOptionalInputs map { _.workflowInput.getName }

    ValidatedMethodConfiguration(methodConfiguration, parsed.validInputs ++ emptyOptionalInputs, parsed.invalidInputs, gatherInputsResult.missingInputs, gatherInputsResult.extraInputs, validatedOutputs, parsed.invalidOutputs)
  }

  // validate a MC, skipping optional empty inputs, and return failure when any inputs/outputs are invalid
  def validateExpressionsForSubmission(methodConfiguration: MethodConfiguration,
                                       gatherInputsResult: GatherInputsResult,
                                       allowRootEntity: Boolean,
                                       parser: SlickExpressionParser): Try[ValidatedMethodConfiguration] = {

    val validated = validateAndParseMCExpressions(methodConfiguration, gatherInputsResult, allowRootEntity, parser)

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
