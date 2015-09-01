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

class EmptyResultException extends RawlsException("Expected single value for workflow input, but evaluated result set was empty")
class MultipleResultException extends RawlsException("Expected single value for workflow input, but evaluated result set had multiple values")
class MissingMandatoryValueException extends RawlsException("Mandatory workflow input is not specified in method config")

object MethodConfigResolver {

  private def getSingleResult(seq: Seq[AttributeValue], optional: Boolean): Try[AttributeValue] = {
    def handleEmpty = if (optional) Success(AttributeNull) else Failure(new EmptyResultException)
    seq match {
      case Seq() => handleEmpty
      case Seq(null) => handleEmpty
      case Seq(AttributeNull) => handleEmpty
      case Seq(single) => Success(single)
      case _ => Failure(new MultipleResultException)
    }
  }

  private def getArrayResult(seq: Seq[AttributeValue]): Try[AttributeValueList] = {
    Success(AttributeValueList(seq.filter(v => v != null && v != AttributeNull)))
  }

  private def unpackResult(mcSequence: Seq[AttributeValue], wfInput: WorkflowInput): Try[Attribute] = wfInput.wdlType match {
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
   * @return A map from input to a Try containing either the unpacked value or a failure
   */
  def resolveInputs(workspaceContext: WorkspaceContext, methodConfig: MethodConfiguration, entity: Entity, wdl: String): Map[String, Try[Attribute]] = {
    NamespaceWithWorkflow.load(wdl, BackendType.LOCAL).workflow.inputs map { wfInput: WorkflowInput =>
      val result = methodConfig.inputs.get(wfInput.fqn) match {
        case Some(AttributeString(expression)) =>
          evaluateResult(workspaceContext, entity, expression) flatMap { mcSequence => unpackResult(mcSequence, wfInput) }
        case _ =>
          if (wfInput.optional) Success(AttributeNull) // if an optional value is unspecified in the MC, we don't care
          else Failure(new MissingMandatoryValueException)
        }
        (wfInput.fqn, result)
      } toMap
    }

  def resolveInputsOrGatherErrors(workspaceContext: WorkspaceContext, methodConfig: MethodConfiguration, entity: Entity, wdl: String): Either[Seq[String], Map[String, Attribute]] = {
    val (successes, failures) = resolveInputs(workspaceContext, methodConfig, entity, wdl) partition (_._2.isSuccess)
    if (failures.nonEmpty) Left( failures collect { case (key, Failure(regret)) => s"Error resolving ${key}: ${regret.getMessage}" } toSeq )
    else Right( successes collect { case (key, Success(value)) => (key, value) } )
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
