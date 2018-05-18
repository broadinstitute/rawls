package org.broadinstitute.dsde.rawls.expressions

import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.model.{AttributeString, ErrorReport, MethodConfiguration, ValidatedMethodConfiguration}
import akka.http.scaladsl.model.StatusCodes

import scala.util.Try

object ExpressionValidator {
  // inputsToParse is parameterized in order to validate in the presence or absence of the associated Method
  // presence: inputs which are both empty and optional are pre-validated, so they are skipped here
  // absence: validate all inputs normally

  private[expressions] def validateAndParse(methodConfiguration: MethodConfiguration, inputsToParse: Map[String, AttributeString], allowRootEntity: Boolean, parser: SlickExpressionParser): ValidatedMethodConfiguration = {
    val (emptyOutputs, outputsToParse) = methodConfiguration.outputs.partition { case (_, expr) => expr.value.isEmpty }

    val parsed = ExpressionParser.parseMCExpressions(inputsToParse, outputsToParse, allowRootEntity, parser)

    // empty output expressions are also valid
    val validatedOutputs = emptyOutputs.keys.toSeq ++ parsed.validOutputs

    ValidatedMethodConfiguration(methodConfiguration, parsed.validInputs, parsed.invalidInputs, validatedOutputs, parsed.invalidOutputs)
  }

  // validate a MC without retrieving the Method from the Repo: assume all inputs are required
  def validateAndParseMCExpressions(methodConfiguration: MethodConfiguration, allowRootEntity: Boolean, parser: SlickExpressionParser): ValidatedMethodConfiguration =
    validateAndParse(methodConfiguration, methodConfiguration.inputs, allowRootEntity, parser)

  // validate a MC, skipping optional empty inputs, and return failure when any inputs/outputs are invalid
  def validateExpressionsForSubmission(methodConfiguration: MethodConfiguration,
                                       methodInputsToParse: Seq[MethodConfigResolver.MethodInput],
                                       emptyOptionalMethodInputs: Seq[MethodConfigResolver.MethodInput],
                                       allowRootEntity: Boolean,
                                       parser: SlickExpressionParser): Try[ValidatedMethodConfiguration] = {
    val inputsToParse = methodInputsToParse map { mi => (mi.workflowInput.localName.value, AttributeString(mi.expression)) }

    val validated = validateAndParse(methodConfiguration, inputsToParse.toMap, allowRootEntity, parser)

    Try {
      if (validated.invalidInputs.nonEmpty || validated.invalidOutputs.nonEmpty) {
        val inputMsg = if (validated.invalidInputs.isEmpty) Seq() else Seq(s"Invalid inputs: ${validated.invalidInputs.mkString(",")}")
        val outputMsg = if (validated.invalidOutputs.isEmpty) Seq() else Seq(s"Invalid outputs: ${validated.invalidOutputs.mkString(",")}")
        val errorStr = (inputMsg ++ outputMsg) mkString " ; "
        throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"Validation errors: $errorStr"))
      }

      // a MethodInput which is both optional and empty is already valid
      val emptyOptionalInputs = emptyOptionalMethodInputs map { _.workflowInput.localName.value }
      validated.copy(validInputs = validated.validInputs ++ emptyOptionalInputs)
    }
  }

}
