package org.broadinstitute.dsde.rawls.entities.local

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.entities.base.ExpressionValidator
import org.broadinstitute.dsde.rawls.expressions.OutputExpression
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.{AntlrTerraExpressionParser, LocalInputExpressionValidationVisitor}
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.GatherInputsResult
import org.broadinstitute.dsde.rawls.model.{AttributeString, ErrorReport, MethodConfiguration, ValidatedMCExpressions, ValidatedMethodConfiguration}

import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.Try

class LocalEntityExpressionValidator(implicit protected val executionContext: ExecutionContext) extends ExpressionValidator {
  // validate a MC, skipping optional empty inputs, and return a ValidatedMethodConfiguration
  def validateMCExpressions(methodConfiguration: MethodConfiguration, gatherInputsResult: GatherInputsResult): Future[ValidatedMethodConfiguration] = {

    Future {
      val inputsToParse = gatherInputsResult.processableInputs map { mi => (mi.workflowInput.getName, AttributeString(mi.expression)) }
      val (emptyOutputs, outputsToParse) = methodConfiguration.outputs.partition { case (_, expr) => expr.value.isEmpty }

      val validated = validateMCExpressionsInternal(
        inputs = inputsToParse.toMap,
        outputs = outputsToParse,
        rootEntityTypeOption = methodConfiguration.rootEntityType
      )

      // empty output expressions are also valid
      val validatedOutputs = emptyOutputs.keys.toSet ++ validated.validOutputs

      // a MethodInput which is both optional and empty is already valid
      val emptyOptionalInputs = gatherInputsResult.emptyOptionalInputs map {
        _.workflowInput.getName
      }

      ValidatedMethodConfiguration(methodConfiguration, validated.validInputs ++ emptyOptionalInputs, validated.invalidInputs, gatherInputsResult.missingInputs, gatherInputsResult.extraInputs, validatedOutputs, validated.invalidOutputs)
    }
  }

  // validate a MC, skipping optional empty inputs, and return failure when any inputs/outputs are invalid
  def validateExpressionsForSubmission(methodConfiguration: MethodConfiguration, gatherInputsResult: GatherInputsResult): Future[Try[ValidatedMethodConfiguration]] = {

    validateMCExpressions(methodConfiguration, gatherInputsResult).map { validated =>

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

  private[local] def validateMCExpressionsInternal(inputs: Map[String, AttributeString],
                                                   outputs: Map[String, AttributeString],
                                                   rootEntityTypeOption: Option[String]): ValidatedMCExpressions = {
    def validateAndPartition(m: Map[String, AttributeString], validateFunc: String => Try[Unit] ) = {
      val validated = m map { case (key, attr) => (key, validateFunc(attr.value)) }
      (
        validated collect { case (key, scala.util.Success(_)) => key } toSet,
        validated collect { case (key, scala.util.Failure(regret)) =>
          regret match {
            case reported: RawlsExceptionWithErrorReport => (key, reported.errorReport.message)
            case _ => (key, regret.getMessage)
          }
        }
      )
    }

    val (successInputs, failedInputs)   = validateAndPartition(inputs, validateInputExpr(rootEntityTypeOption))
    val (successOutputs, failedOutputs) = validateAndPartition(outputs, validateOutputExpr(rootEntityTypeOption))

    ValidatedMCExpressions(successInputs, failedInputs, successOutputs, failedOutputs)
  }

  private def validateInputExpr(rootEntityTypeOption: Option[String])(expression: String): Try[Unit] = {
    val extendedJsonParser = AntlrTerraExpressionParser.getParser(expression)
    val visitor = new LocalInputExpressionValidationVisitor(rootEntityTypeOption.isDefined)

    /*
      parse the expression using ANTLR parser for local input expressions and walk the tree using `visit()` to examine
      child nodes. If it finds an entityLookup node at any point, it fails unless allowRootEntity is true since
      entity expressions are only allowed when running with the workspace data model
     */
    Try(extendedJsonParser.root()).flatMap(visitor.visit)
  }

  private[local] def validateOutputExpr(rootEntityTypeOption: Option[String])(expression: String): Try[Unit] = {
    OutputExpression.validate(expression, rootEntityTypeOption)
  }
}
