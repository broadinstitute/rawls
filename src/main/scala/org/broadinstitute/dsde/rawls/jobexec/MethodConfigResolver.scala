package org.broadinstitute.dsde.rawls.jobexec

import wdl4s.{FullyQualifiedName, WorkflowInput, NamespaceWithWorkflow}
import wdl4s.types.{WdlArrayType}
import org.broadinstitute.dsde.rawls.{model, RawlsException}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import spray.json._
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.slick.ReadAction
import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import org.broadinstitute.dsde.rawls.expressions.SlickExpressionEvaluator
import org.broadinstitute.dsde.rawls.dataaccess.slick.DataAccess

object MethodConfigResolver {
  val emptyResultError = "Expected single value for workflow input, but evaluated result set was empty"
  val multipleResultError  = "Expected single value for workflow input, but evaluated result set had multiple values"
  val missingMandatoryValueError  = "Mandatory workflow input is not specified in method config"

  private def getSingleResult(inputName: String, seq: Iterable[AttributeValue], optional: Boolean): SubmissionValidationValue = {
    def handleEmpty = if (optional) None else Some(emptyResultError)
    seq match {
      case Seq() => SubmissionValidationValue(None, handleEmpty, inputName)
      case Seq(null) => SubmissionValidationValue(None, handleEmpty, inputName)
      case Seq(AttributeNull) => SubmissionValidationValue(None, handleEmpty, inputName)
      case Seq(singleValue) => SubmissionValidationValue(Some(singleValue), None, inputName)
      case multipleValues => SubmissionValidationValue(Some(AttributeValueList(multipleValues.toSeq)), Some(multipleResultError), inputName)
    }
  }

  private def getArrayResult(inputName: String, seq: Iterable[AttributeValue]): SubmissionValidationValue = {
    SubmissionValidationValue(Some(AttributeValueList(seq.filter(v => v != null && v != AttributeNull).toSeq)), None, inputName)
  }

  private def unpackResult(mcSequence: Iterable[AttributeValue], wfInput: WorkflowInput): SubmissionValidationValue = wfInput.wdlType match {
    case arrayType: WdlArrayType => getArrayResult(wfInput.fqn, mcSequence)
    case _ => getSingleResult(wfInput.fqn, mcSequence, wfInput.optional)
  }

  private def evaluateResult(workspaceContext: SlickWorkspaceContext, rootEntity: Entity, expression: String, dataAccess: DataAccess)(implicit executionContext: ExecutionContext): Try[ReadAction[Iterable[AttributeValue]]] = {
    val evaluator = new SlickExpressionEvaluator(dataAccess)
    evaluator.evalFinalAttribute(workspaceContext, rootEntity.entityType, rootEntity.name, expression)
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

  def resolveInputs(workspaceContext: SlickWorkspaceContext, inputs: Seq[MethodInput], entity: Entity, dataAccess: DataAccess)(implicit executionContext: ExecutionContext): ReadAction[Seq[SubmissionValidationValue]] = {
    import dataAccess.driver.api._
    val evaluator = new SlickExpressionEvaluator(dataAccess)
    DBIO.sequence(inputs.map { input =>
      evaluator.evalFinalAttribute(workspaceContext,entity.entityType,entity.name,input.expression) match {
        case Success(attributeSequenceAction) => attributeSequenceAction.map(attributeSequence => unpackResult(attributeSequence.toSeq,input.workflowInput))
        case Failure(regrets) => DBIO.successful(SubmissionValidationValue(None,Some(regrets.getMessage), input.workflowInput.fqn))
      }
    })
  }

  /**
   * Try (1) evaluating inputs, and then (2) unpacking them against WDL.
   *
   * @return A map from input name to a SubmissionValidationValue containing a resolved value and / or error
   */
  def resolveInputs(workspaceContext: SlickWorkspaceContext, methodConfig: MethodConfiguration, entity: Entity, wdl: String, dataAccess: DataAccess)(implicit executionContext: ExecutionContext): ReadAction[Map[String, SubmissionValidationValue]] = {
    import dataAccess.driver.api._
    
    val resolutions = NamespaceWithWorkflow.load(wdl).workflow.inputs map { case (fqn: FullyQualifiedName, wfInput: WorkflowInput) =>
      val result = methodConfig.inputs.get(fqn) match {
        case Some(AttributeString(expression)) =>
          evaluateResult(workspaceContext, entity, expression, dataAccess) match {
            case Success(mcSequence) => mcSequence.map(unpackResult(_, wfInput))
            case Failure(regret) => DBIO.successful(SubmissionValidationValue(None, Some(regret.getMessage), fqn))
          }
        case _ =>
          val errorOption = if (wfInput.optional) None else Some(missingMandatoryValueError) // if an optional value is unspecified in the MC, we don't care
          DBIO.successful(SubmissionValidationValue(None, errorOption, fqn))
      }
      result.map((fqn, _))
    }
    
    DBIO.sequence(resolutions).map(_.toMap)
  }

  /**
   * Try resolving inputs. If there are any failures, ONLY return the error messages.
   * Otherwise extract the resolved values (excluding empty / None values) and return those.
   */
  def resolveInputsOrGatherErrors(workspaceContext: SlickWorkspaceContext, methodConfig: MethodConfiguration, entity: Entity, wdl: String, dataAccess: DataAccess)(implicit executionContext: ExecutionContext): ReadAction[Either[Seq[String], Map[String, Attribute]]] = {
    resolveInputs(workspaceContext, methodConfig, entity, wdl, dataAccess) map { resolutions =>
      val (successes, failures) = resolutions.partition (_._2.error.isEmpty)
      if (failures.nonEmpty) Left( failures collect { case (key, SubmissionValidationValue(_, Some(error), inputName)) => s"Error resolving ${inputName}: ${error}" } toSeq )
      else Right( successes collect { case (key, SubmissionValidationValue(Some(attribute), _, inputName)) => (inputName, attribute) } )
    }
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
