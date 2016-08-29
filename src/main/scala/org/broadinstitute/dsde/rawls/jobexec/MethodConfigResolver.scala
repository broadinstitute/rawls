package org.broadinstitute.dsde.rawls.jobexec

import org.broadinstitute.dsde.rawls.util.CollectionUtils
import slick.dbio
import slick.dbio.{NoStream, DBIOAction}
import slick.dbio.Effect.Read
import wdl4s.{FullyQualifiedName, WorkflowInput, NamespaceWithWorkflow}
import wdl4s.types.{WdlArrayType}
import org.broadinstitute.dsde.rawls.{model, RawlsException}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import spray.json._
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import org.broadinstitute.dsde.rawls.expressions.SlickExpressionEvaluator

object MethodConfigResolver {
  val emptyResultError = "Expected single value for workflow input, but evaluated result set was empty"
  val multipleResultError  = "Expected single value for workflow input, but evaluated result set had multiple values"
  val missingMandatoryValueError  = "Mandatory workflow input is not specified in method config"

  private def getSingleResult(inputName: String, seq: Iterable[AttributeValue], optional: Boolean): SubmissionValidationValue = {
    def handleEmpty = if (optional) None else Some(emptyResultError)
    seq match {
      case Seq() => SubmissionValidationValue(None, handleEmpty, DefaultAttributeName(inputName))
      case Seq(null) => SubmissionValidationValue(None, handleEmpty, DefaultAttributeName(inputName))
      case Seq(AttributeNull) => SubmissionValidationValue(None, handleEmpty, DefaultAttributeName(inputName))
      case Seq(singleValue) => SubmissionValidationValue(Some(singleValue), None, DefaultAttributeName(inputName))
      case multipleValues => SubmissionValidationValue(Some(AttributeValueList(multipleValues.toSeq)), Some(multipleResultError), DefaultAttributeName(inputName))
    }
  }

  private def getArrayResult(inputName: String, seq: Iterable[AttributeValue]): SubmissionValidationValue = {
    val filterSeq = seq.filter(v => v != null && v != AttributeNull).toSeq
    if(filterSeq.isEmpty) {
      SubmissionValidationValue(Some(AttributeEmptyList), None, DefaultAttributeName(inputName))
    } else {
      SubmissionValidationValue(Some(AttributeValueList(filterSeq)), None, DefaultAttributeName(inputName))
    }
  }

  private def unpackResult(mcSequence: Iterable[AttributeValue], wfInput: WorkflowInput): SubmissionValidationValue = wfInput.wdlType match {
    case arrayType: WdlArrayType => getArrayResult(wfInput.fqn, mcSequence)
    case _ => getSingleResult(wfInput.fqn, mcSequence, wfInput.optional)
  }

  case class MethodInput(workflowInput: WorkflowInput, expression: String)

  def gatherInputs(methodConfig: MethodConfiguration, wdl: String): Seq[MethodInput] = {
    val agoraInputs = NamespaceWithWorkflow.load(wdl).workflow.inputs
    val missingInputs = agoraInputs.filter{ case (fqn,workflowInput) => !methodConfig.inputs.contains(fqn) && !workflowInput.optional }.keys
    val extraInputs = methodConfig.inputs.filter{ case (name,expression) => !agoraInputs.contains(name) }.keys
    if ( missingInputs.size > 0 || extraInputs.size > 0 ) {
      val message =
        if ( missingInputs.size > 0 )
          if ( extraInputs.size > 0 )
            "is missing definitions for these inputs: " + missingInputs.mkString(", ") + " and it has extraneous definitions for these inputs: " + extraInputs.mkString(", ")
          else
            "is missing definitions for these inputs: " + missingInputs.mkString(", ")
        else
          "has extraneous definitions for these inputs: " + extraInputs.mkString(", ")
      throw new RawlsException(s"MethodConfiguration ${methodConfig.namespace}/${methodConfig.name} ${message}")
    }
    for ( (name,expression) <- methodConfig.inputs.toSeq ) yield MethodInput(agoraInputs.get(name).get,expression.value)
  }

  def resolveInputsForEntities(workspaceContext: SlickWorkspaceContext, inputs: Seq[MethodInput], entities: Seq[EntityRecord], dataAccess: DataAccess)(implicit executionContext: ExecutionContext): ReadWriteAction[Map[String, Seq[SubmissionValidationValue]]] = {
    import dataAccess.driver.api._

    if( inputs.isEmpty ) {
      //no inputs to resolve = just return an empty map back!
      DBIO.successful(entities.map( _.name -> Seq.empty[SubmissionValidationValue] ).toMap)
    } else {
      SlickExpressionEvaluator.withNewExpressionEvaluator(dataAccess, entities) { evaluator =>
        //Evaluate the results per input and return a seq of DBIO[ Map(entity -> value) ], one per input
        val resultsByInput = inputs.map { input =>
          evaluator.evalFinalAttribute(workspaceContext, input.expression).asTry.map { tryAttribsByEntity =>
            val validationValuesByEntity: Seq[(String, SubmissionValidationValue)] = tryAttribsByEntity match {
              case Failure(regret) =>
                //The DBIOAction failed - this input expression was unparseable. Make an error for each entity.
                entities.map(e => (e.name, SubmissionValidationValue(None, Some(regret.getMessage), DefaultAttributeName(input.workflowInput.fqn))))
              case Success(attributeMap) =>
                //The expression was parseable, but that doesn't mean we got results...
                attributeMap.map {
                  case (key, Success(attrSeq)) => key -> unpackResult(attrSeq.toSeq, input.workflowInput)
                  case (key, Failure(regret)) => key -> SubmissionValidationValue(None, Some(regret.getMessage), DefaultAttributeName(input.workflowInput.fqn))
                }.toSeq
            }
            validationValuesByEntity
          }
        }

        //Flip the list of DBIO monads into one on the outside that we can map across and then group by entity.
        DBIO.sequence(resultsByInput) map { results =>
          CollectionUtils.groupByTuples(results.flatten)
        }
      }
    }
  }

  /**
   * Convert result of resolveInputs to WDL input format, ignoring AttributeNulls.
   * @return serialized JSON to send to Cromwell
   */
  def propertiesToWdlInputs(inputs: Map[AttributeName, Attribute]): String = JsObject(
    inputs flatMap {
      case (key, AttributeNull) => None
      case (key, notNullValue) => Some((AttributeName.toDelimitedName(key), notNullValue.toJson))   // TODO can Cromwell handle colons?
    }
  ) toString

  def toMethodConfiguration(wdl: String, methodRepoMethod: MethodRepoMethod) = {
    val workflow = NamespaceWithWorkflow.load(wdl).workflow
    val nothing = AttributeString("expression")
    val inputs = for ( (fqn: FullyQualifiedName, wfInput: WorkflowInput) <- workflow.inputs ) yield fqn.toString -> nothing
    val outputs = workflow.outputs map (o =>  o.fullyQualifiedName.toString -> nothing)
    MethodConfiguration("namespace","name","rootEntityType",Map(),inputs.toMap,outputs.toMap,methodRepoMethod)
  }

  def getMethodInputsOutputs(wdl: String) = {
    val workflow = NamespaceWithWorkflow.load(wdl).workflow
    MethodInputsOutputs(
      (workflow.inputs map {case (fqn: FullyQualifiedName, wfInput:WorkflowInput) => model.MethodInput(fqn, wfInput.wdlType.toWdlString, wfInput.optional)}).toSeq,
      workflow.outputs.map(o => MethodOutput(o.fullyQualifiedName, o.wdlType.toWdlString)).toSeq)
  }
}
