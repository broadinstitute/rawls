package org.broadinstitute.dsde.rawls.jobexec

import cromwell.binding.types.{WdlType, WdlArrayType}
import cromwell.binding.{WorkflowInput, NamespaceWithWorkflow, WdlNamespace}
import cromwell.engine.backend.Backend
import cromwell.parser.BackendType
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.{WorkspaceContext, RawlsTransaction}
import org.broadinstitute.dsde.rawls.expressions.{ExpressionParser, ExpressionEvaluator}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import spray.httpx.SprayJsonSupport._
import spray.json._

import scala.util.{Failure, Success, Try}

object MethodConfigResolver {
  val emptyResultError = "Expected single value for workflow input, but evaluated result set was empty"
  val multipleResultError  = "Expected single value for workflow input, but evaluated result set had multiple values"
  val missingMandatoryValueError  = "Mandatory workflow input is not specified in method config"

  private def getSingleResult(seq: Seq[AttributeValue], optional: Boolean): SubmissionValidationValue = {
    def handleEmpty = if (optional) None else Some(emptyResultError)
    seq match {
      case Seq() => SubmissionValidationValue(None, handleEmpty)
      case Seq(null) => SubmissionValidationValue(None, handleEmpty)
      case Seq(AttributeNull) => SubmissionValidationValue(None, handleEmpty)
      case Seq(singleValue) => SubmissionValidationValue(Some(singleValue), None)
      case multipleValues => SubmissionValidationValue(Some(AttributeValueList(multipleValues)), Some(multipleResultError))
    }
  }

  private def getArrayResult(seq: Seq[AttributeValue]): SubmissionValidationValue = {
    SubmissionValidationValue(Some(AttributeValueList(seq.filter(v => v != null && v != AttributeNull))), None)
  }

  private def unpackResult(mcSequence: Seq[AttributeValue], wfInput: WorkflowInput): SubmissionValidationValue = wfInput.wdlType match {
    case arrayType: WdlArrayType => getArrayResult(mcSequence)
    case _ => getSingleResult(mcSequence, wfInput.optional)
  }

  private def evaluateResult(workspaceContext: WorkspaceContext, rootEntity: Entity, expression: String): Try[Seq[AttributeValue]] = {
    val evaluator = new ExpressionEvaluator(new ExpressionParser)
    evaluator.evalFinalAttribute(workspaceContext, rootEntity.entityType, rootEntity.name, expression)
  }

  /**
   * Try (1) evaluating inputs, and then (2) unpacking them against WDL.
   *
   * @return A map from input name to a SubmissionValidationValue containing a resolved value and / or error
   */
  def resolveInputs(workspaceContext: WorkspaceContext, methodConfig: MethodConfiguration, entity: Entity, wdl: String): Map[String, SubmissionValidationValue] = {
    NamespaceWithWorkflow.load(wdl, BackendType.LOCAL).workflow.inputs map { wfInput: WorkflowInput =>
      val result = methodConfig.inputs.get(wfInput.fqn) match {
        case Some(AttributeString(expression)) =>
          evaluateResult(workspaceContext, entity, expression) match {
            case Success(mcSequence) => unpackResult(mcSequence, wfInput)
            case Failure(regret) => SubmissionValidationValue(None, Some(regret.getMessage))
          }
        case _ =>
          val errorOption = if (wfInput.optional) None else Some(missingMandatoryValueError) // if an optional value is unspecified in the MC, we don't care
          SubmissionValidationValue(None, errorOption)
        }
        (wfInput.fqn, result)
      } toMap
    }

  /**
   * Try resolving inputs. If there are any failures, ONLY return the error messages.
   * Otherwise extract the resolved values (excluding empty / None values) and return those.
   */
  def resolveInputsOrGatherErrors(workspaceContext: WorkspaceContext, methodConfig: MethodConfiguration, entity: Entity, wdl: String): Either[Seq[String], Map[String, Attribute]] = {
    val (successes, failures) = resolveInputs(workspaceContext, methodConfig, entity, wdl) partition (_._2.error.isEmpty)
    if (failures.nonEmpty) Left( failures collect { case (key, SubmissionValidationValue(_, Some(error))) => s"Error resolving ${key}: ${error}" } toSeq )
    else Right( successes collect { case (key, SubmissionValidationValue(Some(attribute), _)) => (key, attribute) } )
  }

  /**
   * Convert result of resolveInputs to WDL input format, ignoring AttributeNulls.
   * @return serialized JSON to send to Cromwell
   */
  def propertiesToWdlInputs(inputs: Map[String, Attribute]): String = JsObject(
    inputs flatMap {
      case (key, AttributeNull) => None
      case (key, notNullValue) => Some((key, notNullValue.toJson))
    }
  ) toString

  def toMethodConfiguration(wdl: String, methodRepoMethod: MethodRepoMethod) = {
    val workflow = NamespaceWithWorkflow.load(wdl, BackendType.LOCAL).workflow
    val nothing = AttributeString("expression")
    val inputs = for ( input <- workflow.inputs ) yield input.fqn.toString -> nothing
    val outputs = for ( output <- workflow.outputs ) yield output._1.toString -> nothing
    MethodConfiguration("namespace","name","rootEntityType",Map(),inputs.toMap,outputs,MethodRepoConfiguration("none","none","none"),methodRepoMethod)
  }
}
