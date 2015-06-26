package org.broadinstitute.dsde.rawls.jobexec

import cromwell.binding.WdlNamespace
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.RawlsTransaction
import org.broadinstitute.dsde.rawls.expressions.{ExpressionParser, ExpressionEvaluator}
import org.broadinstitute.dsde.rawls.model.{Attribute, AttributeConversions, Entity, MethodConfiguration}
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
    case Seq(single) => Success(single)
    case _ => Failure(new MultipleResultException)
  }

  /**
   * Evaluate expressions in a method config against a root entity.
   * @return map from input name to a Try containing a resolved value or failure
   */
  def resolveInputs(methodConfig: MethodConfiguration, entity: Entity, wdl: String, txn: RawlsTransaction): Map[String, Try[Any]] = {
    val wdlInputs = WdlNamespace.load(wdl).workflows.head.inputs
    txn withGraph { graph =>
      val evaluator = new ExpressionEvaluator(graph, new ExpressionParser)
      methodConfig.inputs.map {
        case (name, expression) => {
          val evaluated = evaluator.evalFinalAttribute(entity.workspaceName.namespace, entity.workspaceName.name, entity.entityType, entity.name, expression)
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
  def getValidationErrors(methodConfig: MethodConfiguration, entity: Entity, wdl: String, txn: RawlsTransaction) = {
    val configInputs = resolveInputs(methodConfig, entity, wdl, txn)
    WdlNamespace.load(wdl).workflows.head.inputs.map {
      case (wdlName, wdlType) => {
        val errorOption = configInputs.get(wdlName) match {
          case Some(Success(rawValue)) => {
            // TODO: currently Cromwell's exposed type-checking is not very robust.
            // TODO: once Cromwell has a validation endpoint, use that; until then, don't bother checking types.
            None
          }
          case Some(Failure(regret)) => Option(s"Expression eval failure ${regret.getMessage}")
          case None => Option("WDL binding failure: Input is missing from method config")
        }
        (wdlName, errorOption)
      }
    } collect { case (name, Some(error)) => (name, error) }
  }

  def resolveInputsAndAggregateErrors(methodConfig: MethodConfiguration, entity: Entity, wdl: String, txn: RawlsTransaction): Try[Map[String, Any]] = {
    val resolved = resolveInputs(methodConfig, entity, wdl, txn)
    resolved.count(_._2.isFailure) match {
      case 0 => Success(resolved collect { case (name, Success(value)) => (name, value) } )
      case numFails => Failure(new RawlsException(s"There were $numFails failed expression evaluations"))
    }
  }

  def propertiesToWdlInputs(inputs: Map[String, Any]): String = JsObject(
    inputs map { case (name, value) => (name, AttributeConversions.propertyToAttribute(value).asInstanceOf[Attribute].toJson) }
  ) toString

}
