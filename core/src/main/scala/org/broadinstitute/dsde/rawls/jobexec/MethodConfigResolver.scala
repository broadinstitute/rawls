package org.broadinstitute.dsde.rawls.jobexec
import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.expressions.ExpressionEvaluator
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.CollectionUtils
import org.broadinstitute.dsde.rawls.{RawlsException, model}
import spray.json._
import wdl4s.parser.WdlParser.SyntaxError
import wdl4s.types.{WdlArrayType, WdlOptionalType}
import wdl4s.{FullyQualifiedName, WdlNamespaceWithWorkflow, WorkflowInput}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

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
    val notNull = seq.filter(v => v != null && v != AttributeNull)
    val attr = if (notNull.isEmpty) Option(AttributeValueEmptyList) else Option(AttributeValueList(notNull.toSeq))
    SubmissionValidationValue(attr, None, inputName)
  }

  private def unpackResult(mcSequence: Iterable[AttributeValue], wfInput: WorkflowInput): SubmissionValidationValue = wfInput.wdlType match {
    case arrayType: WdlArrayType => getArrayResult(wfInput.fqn, mcSequence)
    case WdlOptionalType(_:WdlArrayType) => getArrayResult(wfInput.fqn, mcSequence) //send optional-arrays down the same codepath as arrays
    case _ => getSingleResult(wfInput.fqn, mcSequence, wfInput.optional)
  }

  def parseWDL(wdl: String): Try[wdl4s.Workflow] = {
    val parsed: Try[WdlNamespaceWithWorkflow] = WdlNamespaceWithWorkflow.load(wdl, Seq()).recoverWith { case t: SyntaxError =>
      Failure(new RawlsException("Failed to parse WDL: " + t.getMessage()))
    }

    parsed map( _.workflow )
  }

  case class MethodInput(workflowInput: WorkflowInput, expression: String)

  def gatherInputs(methodConfig: MethodConfiguration, wdl: String): Try[Seq[MethodInput]] = parseWDL(wdl) map { workflow =>
    def isAttributeEmpty(fqn: FullyQualifiedName): Boolean = {
      methodConfig.inputs.get(fqn) match {
        case Some(AttributeString(value)) => value.isEmpty
        case _ => throw new RawlsException(s"MethodConfiguration ${methodConfig.namespace}/${methodConfig.name} input ${fqn} value is unavailable")
      }
    }
    val agoraInputs = workflow.inputs
    val missingInputs = agoraInputs.filter { case (fqn, workflowInput) => (!methodConfig.inputs.contains(fqn) || isAttributeEmpty(fqn)) && !workflowInput.optional }.keys
    val extraInputs = methodConfig.inputs.filter { case (name, expression) => !agoraInputs.contains(name) }.keys
    if (missingInputs.nonEmpty || extraInputs.nonEmpty) {
      val message =
        if (missingInputs.nonEmpty)
          if (extraInputs.nonEmpty)
            "is missing definitions for these inputs: " + missingInputs.mkString(", ") + " and it has extraneous definitions for these inputs: " + extraInputs.mkString(", ")
          else
            "is missing definitions for these inputs: " + missingInputs.mkString(", ")
        else
          "has extraneous definitions for these inputs: " + extraInputs.mkString(", ")
      throw new RawlsException(s"MethodConfiguration ${methodConfig.namespace}/${methodConfig.name} ${message}")
    }
    for ((name, expression) <- methodConfig.inputs.toSeq) yield MethodInput(agoraInputs(name), expression.value)
  }

  def evaluateInputExpressions(workspaceContext: SlickWorkspaceContext, inputs: Seq[MethodInput], entities: Seq[EntityRecord], dataAccess: DataAccess)(implicit executionContext: ExecutionContext): ReadWriteAction[Map[String, Seq[SubmissionValidationValue]]] = {
    import dataAccess.driver.api._

    if( inputs.isEmpty ) {
      //no inputs to evaluate = just return an empty map back!
      DBIO.successful(entities.map( _.name -> Seq.empty[SubmissionValidationValue] ).toMap)
    } else {
      ExpressionEvaluator.withNewExpressionEvaluator(dataAccess, entities) { evaluator =>
        //Evaluate the results per input and return a seq of DBIO[ Map(entity -> value) ], one per input
        val resultsByInput = inputs.map { input =>
          evaluator.evalFinalAttribute(workspaceContext, input.expression).asTry.map { tryAttribsByEntity =>
            val validationValuesByEntity: Seq[(String, SubmissionValidationValue)] = tryAttribsByEntity match {
              case Failure(regret) =>
                //The DBIOAction failed - this input expression was not evaluated. Make an error for each entity.
                entities.map(e => (e.name, SubmissionValidationValue(None, Some(regret.getMessage), input.workflowInput.fqn)))
              case Success(attributeMap) =>
                //The expression was evaluated, but that doesn't mean we got results...
                attributeMap.map {
                  case (key, Success(attrSeq)) => key -> unpackResult(attrSeq.toSeq, input.workflowInput)
                  case (key, Failure(regret)) => key -> SubmissionValidationValue(None, Some(regret.getMessage), input.workflowInput.fqn)
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
  def propertiesToWdlInputs(inputs: Map[String, Attribute]): String = JsObject(
    inputs flatMap {
      case (key, AttributeNull) => None
      case (key, notNullValue) => Some(key, notNullValue.toJson(WDLJsonSupport.attributeFormat))
    }
  ) toString

  def toMethodConfiguration(wdl: String, methodRepoMethod: MethodRepoMethod): Try[MethodConfiguration] = parseWDL(wdl) map { workflow =>
    val nothing = AttributeString("")
    val inputs = for ((fqn: FullyQualifiedName, wfInput: WorkflowInput) <- workflow.inputs) yield fqn.toString -> nothing
    val outputs = workflow.outputs map (o => o.locallyQualifiedName(workflow) -> nothing)
    MethodConfiguration("namespace", "name", "rootEntityType", Map(), inputs.toMap, outputs.toMap, methodRepoMethod)
  }

  def getMethodInputsOutputs(wdl: String): Try[MethodInputsOutputs] = parseWDL(wdl) map { workflow =>
    MethodInputsOutputs(
      (workflow.inputs map { case (fqn: FullyQualifiedName, wfInput: WorkflowInput) => model.MethodInput(fqn, wfInput.wdlType.toWdlString, wfInput.optional) }).toSeq,
      workflow.outputs.map(o => MethodOutput(o.locallyQualifiedName(workflow), o.wdlType.toWdlString)))
  }
}
