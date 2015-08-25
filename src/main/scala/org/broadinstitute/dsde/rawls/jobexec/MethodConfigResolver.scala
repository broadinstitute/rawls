package org.broadinstitute.dsde.rawls.jobexec

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

class EmptyResultException extends RawlsException("Expected one value, but found none")
class MultipleResultException extends RawlsException("Expected one value, but found multiple")

object MethodConfigResolver {

  private def getSingleResult[T](seq: Seq[T]): Try[T] = seq match {
    case Seq() => Failure(new EmptyResultException)
    case Seq(null) => Failure(new EmptyResultException) // sometimes expression eval returns a Seq(null) for an empty result
    case Seq(AttributeNull) => Failure(new EmptyResultException)
    case Seq(single) => Success(single)
    case _ => Failure(new MultipleResultException)
  }

  /**
   * Evaluate expressions in a method config against a root entity.
   * @return map from input name to a Try containing a resolved value or failure
   */
  def resolveInputs(workspaceContext: WorkspaceContext, methodConfig: MethodConfiguration, entity: Entity, wdl: String, txn: RawlsTransaction): Map[String, Try[Attribute]] = {
    val wdlInputs = NamespaceWithWorkflow.load(wdl, BackendType.LOCAL).workflow.inputs.map((w:WorkflowInput) => w.fqn -> w.wdlType).toMap
    txn withGraph { graph =>
      val evaluator = new ExpressionEvaluator(new ExpressionParser)
      methodConfig.inputs.map {
        case (name, expression) => {
          val evaluated = evaluator.evalFinalAttribute(workspaceContext, entity.entityType, entity.name, expression.value)
          // now look at expected WDL type to see if we should unpack a single value from the Seq[Any]
          val unpacked = evaluated.flatMap(sequence => wdlInputs.get(name) match {
              // TODO: Cromwell doesn't yet have compound types (e.g. Seq)
              // TODO: so until it does just take the head
              case Some(wdlType) => getSingleResult(sequence)
              case None => getSingleResult(sequence)
            }
          )
          (name, unpacked)
        }
      }
    }
  }

  /**
   * Get errors in both expression eval and WDL binding.
   * @return map of from input name to error message (so if the map is empty, the config is valid)
   */
  def getValidationErrors(workspaceContext: WorkspaceContext, methodConfig: MethodConfiguration, entity: Entity, wdl: String, txn: RawlsTransaction) = {
    val configInputs = resolveInputs(workspaceContext, methodConfig, entity, wdl, txn)
    NamespaceWithWorkflow.load(wdl, BackendType.LOCAL).workflow.inputs.map {
      case w:WorkflowInput => {
        val errorOption = configInputs.get(w.fqn) match {
          case Some(Success(rawValue)) => {
            // TODO: currently Cromwell's exposed type-checking is not very robust.
            // TODO: once Cromwell has a validation endpoint, use that; until then, don't bother checking types.
            None
          }
          case Some(Failure(regret)) => Option(s"Expression eval failure ${regret.getMessage}")
          case None => Option("WDL binding failure: Input is missing from method config")
        }
        (w.fqn, errorOption)
      }
    } collect { case (name, Some(error)) => name -> error } toMap
  }

  def resolveInputsAndAggregateErrors(workspaceContext: WorkspaceContext, methodConfig: MethodConfiguration, entity: Entity, wdl: String, txn: RawlsTransaction): Try[Map[String, Any]] = {
    val resolved = resolveInputs(workspaceContext, methodConfig, entity, wdl, txn)
    resolved.count(_._2.isFailure) match {
      case 0 => Success(resolved collect { case (name, Success(value)) => (name, value) } )
      case numFails => Failure(new RawlsException(s"There were $numFails failed expression evaluations"))
    }
  }

  def propertiesToWdlInputs(inputs: Map[String, Attribute]): String = JsObject(
    inputs map { case (name, value) => (name, value.toJson) }
  ) toString

  def toMethodConfiguration(wdl: String, methodRepoMethod: MethodRepoMethod) = {
    val workflow = NamespaceWithWorkflow.load(wdl, BackendType.LOCAL).workflow
    val nothing = AttributeString("expression")
    val inputs = for ( input <- workflow.inputs ) yield input.fqn.toString -> nothing
    val outputs = for ( output <- workflow.outputs ) yield output._1.toString -> nothing
    MethodConfiguration("namespace","name","rootEntityType",Map(),inputs.toMap,outputs,MethodRepoConfiguration("none","none","none"),methodRepoMethod)
  }
}
