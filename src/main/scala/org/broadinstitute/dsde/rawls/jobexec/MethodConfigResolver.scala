package org.broadinstitute.dsde.rawls.jobexec

import cromwell.binding.types.WdlArrayType
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
class MissingMandatoryValueException extends RawlsException("Mandatory workflow input is missing or fails to evaluate in method config")

object MethodConfigResolver {

  private def getSingleResult(seq: Seq[AttributeValue]): Try[AttributeValue] = seq match {
    case Seq() => Failure(new EmptyResultException)
    case Seq(null) => Failure(new EmptyResultException) // sometimes expression eval returns a Seq(null) for an empty result
    case Seq(AttributeNull) => Failure(new EmptyResultException)
    case Seq(single) => Success(single)
    case _ => Failure(new MultipleResultException)
  }

  private def getArrayResult(seq: Seq[AttributeValue]): Try[AttributeValueList] = {
    // TODO figure out if this is the right approach to filtering nulls.
    Success(AttributeValueList(seq.filter(v => v != null && v != AttributeNull)))
  }

  /**
   * Evaluate expressions in a method config against a root entity.
   * @return map from method config input name to a Try containing a resolved value or failure
   */
  def evaluateMethodConfigInputs(workspaceContext: WorkspaceContext, methodConfig: MethodConfiguration, entity: Entity, txn: RawlsTransaction): Map[String, Try[Seq[AttributeValue]]] = {
    txn withGraph { graph =>
      val evaluator = new ExpressionEvaluator(new ExpressionParser)
      methodConfig.inputs.map {
        case (name, expression) => {
          val evaluated = evaluator.evalFinalAttribute(workspaceContext, entity.entityType, entity.name, expression.value)
          (name, evaluated)
        }
      }
    }
  }

  /**
   * Unpack evaluated method config inputs using WDL.
   * @return map from workflow input name to a Try containing an unpacked value or failure
   */
  def unpackWorkflowInputs(mcInputs: Map[String, Try[Seq[AttributeValue]]], wdl: String): Map[String, Try[Attribute]] = {
    val wdlNamespace = NamespaceWithWorkflow.load(wdl, BackendType.LOCAL)
    val workflowInputs = wdlNamespace.workflow.inputs
    val taskInputs = wdlNamespace.tasks.flatMap(_.inputs)
    NamespaceWithWorkflow.load(wdl, BackendType.LOCAL).workflow.inputs map { wfInput: WorkflowInput =>
      val mcInputOption = mcInputs.get(wfInput.fqn) match {
        case Some(Success(input)) => Some(input)
        case _ => None // if this input is missing OR failed to evaluate, pass None here
      }
      val unpacked = mcInputOption match {
        case None =>
          if (wfInput.optional) Success(AttributeNull)
          else Failure(new MissingMandatoryValueException)
        case Some(mcSequence) =>
          wfInput.wdlType match {
            case arrayType: WdlArrayType => getArrayResult(mcSequence)
            case _ => getSingleResult(mcSequence)
          }
      }
      (wfInput.fqn, unpacked)
    } toMap
  }

  /**
   * @return map from input to resolved value or failure for each WDL input
   */
  def resolveInputs(workspaceContext: WorkspaceContext, methodConfig: MethodConfiguration, entity: Entity, wdl: String, txn: RawlsTransaction): Map[String, Try[Attribute]] = {
    val evaluated = evaluateMethodConfigInputs(workspaceContext, methodConfig, entity, txn)
    val unpacked = unpackWorkflowInputs(evaluated, wdl)
    unpacked
  }

  /**
   * Convert result of resolveInputs to WDL input format, ignoring AttributeNulls and Failures
   * @return serialized JSON to send to Cromwell
   */
  def propertiesToWdlInputs(inputs: Map[String, Try[Attribute]]): String = JsObject(
    inputs collect { case (name, Success(value)) => (name, value.toJson) }
  ) toString

  def toMethodConfiguration(wdl: String, methodRepoMethod: MethodRepoMethod) = {
    val workflow = NamespaceWithWorkflow.load(wdl, BackendType.LOCAL).workflow
    val nothing = AttributeString("expression")
    val inputs = for ( input <- workflow.inputs ) yield input.fqn.toString -> nothing
    val outputs = for ( output <- workflow.outputs ) yield output._1.toString -> nothing
    MethodConfiguration("namespace","name","rootEntityType",Map(),inputs.toMap,outputs,MethodRepoConfiguration("none","none","none"),methodRepoMethod)
  }
}
