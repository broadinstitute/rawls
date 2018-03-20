package org.broadinstitute.dsde.rawls.jobexec
import org.broadinstitute.dsde.rawls.dataaccess.{MarthaDAO, SlickWorkspaceContext}
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.expressions.ExpressionEvaluator
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.CollectionUtils
import org.broadinstitute.dsde.rawls.{RawlsException, model}
import spray.json._
import wdl4s.parser.WdlParser.SyntaxError
import wom.callable.Callable.InputDefinition
import wom.types.{WomArrayType, WomOptionalType}
import wdl.{FullyQualifiedName, WdlNamespaceWithWorkflow, WdlWorkflow}
import wdl.WdlNamespace.httpResolver

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object MethodConfigResolver {
  val emptyResultError = "Expected single value for workflow input, but evaluated result set was empty"
  val multipleResultError  = "Expected single value for workflow input, but evaluated result set had multiple values"
  val missingMandatoryValueError  = "Mandatory workflow input is not specified in method config"

  def mapDosToGs(value: AttributeValue, marthaDAO: MarthaDAO)(implicit executionContext: ExecutionContext): Future[AttributeValue] = {
    value match {
      case s: AttributeString if s.value.matches(marthaDAO.dosUriPattern) => marthaDAO.dosToGs(s.value) map { AttributeString }
      case v: AttributeValue => Future.successful[AttributeValue](v)
    }
  }

  def mapDosToGs(values: Iterable[AttributeValue], marthaDAO: MarthaDAO)(implicit executionContext: ExecutionContext): Future[Seq[AttributeValue]] = {
    Future.traverse(values.toSeq) { mapDosToGs(_, marthaDAO) }
  }

  private def getSingleResult(inputName: String, seq: Iterable[AttributeValue], optional: Boolean, marthaDAO: MarthaDAO)(implicit executionContext: ExecutionContext): Future[SubmissionValidationValue] = {
    def handleEmpty = if (optional) None else Some(emptyResultError)
    seq match {
      case Seq() => Future.successful(SubmissionValidationValue(None, handleEmpty, inputName))
      case Seq(null) => Future.successful(SubmissionValidationValue(None, handleEmpty, inputName))
      case Seq(AttributeNull) => Future.successful(SubmissionValidationValue(None, handleEmpty, inputName))
      case Seq(singleValue) => mapDosToGs(singleValue, marthaDAO) map { v => SubmissionValidationValue(Some(v), None, inputName) }
      case multipleValues => mapDosToGs(multipleValues, marthaDAO) map { vs => SubmissionValidationValue(Some(AttributeValueList(vs)), Some(multipleResultError), inputName) }
    }
  }

  private def getArrayResult(inputName: String, seq: Iterable[AttributeValue], marthaDAO: MarthaDAO)(implicit executionContext: ExecutionContext): Future[SubmissionValidationValue] = {
    val notNull: Seq[AttributeValue] = seq.filter(v => v != null && v != AttributeNull).toSeq
    val attr = notNull match {
      case Nil => Future.successful(Option(AttributeValueEmptyList))
      //GAWB-2509: don't pack single-elem RawJson array results into another layer of array
      //NOTE: This works, except for the following situation: a participant with a RawJson double-array attribute, in a single-element participant set.
      // Evaluating this.participants.raw_json on the pset will incorrectly hit this case and return a 2D array when it should return a 3D array.
      // The true fix for this is to look into why the slick expression evaluator wraps deserialized AttributeValues in a Seq, and instead
      // return the proper result type, removing the need to infer whether it's a scalar or array type from the WDL input.
      case AttributeValueRawJson(JsArray(_)) +: Seq() => Future.successful(Option(notNull.head))
      case _ => mapDosToGs(notNull, marthaDAO) map { v => Option(AttributeValueList(v)) }
    }
    attr map { a => SubmissionValidationValue(a, None, inputName) }
  }

  private def unpackResult(mcSequence: Iterable[AttributeValue], wfInput: InputDefinition, marthaDAO: MarthaDAO)(implicit executionContext: ExecutionContext): Future[SubmissionValidationValue] = wfInput.womType match {
    case arrayType: WomArrayType => getArrayResult(wfInput.localName.value, mcSequence, marthaDAO)
    case WomOptionalType(_:WomArrayType) => getArrayResult(wfInput.localName.value, mcSequence, marthaDAO) //send optional-arrays down the same codepath as arrays
    case _ => getSingleResult(wfInput.localName.value, mcSequence, wfInput.optional, marthaDAO)
  }

  def parseWDL(wdl: String): Try[WdlWorkflow] = {
    val parsed: Try[WdlNamespaceWithWorkflow] = WdlNamespaceWithWorkflow.load(wdl, Seq(httpResolver(_))).recoverWith { case t: SyntaxError =>
      Failure(new RawlsException("Failed to parse WDL: " + t.getMessage()))
    }

    parsed map( _.workflow )
  }

  case class MethodInput(workflowInput: InputDefinition, expression: String)

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

  def evaluateInputExpressions(workspaceContext: SlickWorkspaceContext, inputs: Seq[MethodInput], entities: Seq[EntityRecord], dataAccess: DataAccess, marthaDAO: MarthaDAO)(implicit executionContext: ExecutionContext): ReadWriteAction[Map[String, Seq[SubmissionValidationValue]]] = {
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
                entities.map(e => (e.name, SubmissionValidationValue(None, Some(regret.getMessage), input.workflowInput.localName.value)))
              case Success(attributeMap) =>
                //The expression was evaluated, but that doesn't mean we got results...
                attributeMap.map {
                  case (key, Success(attrSeq)) => key -> Await.result(unpackResult(attrSeq.toSeq, input.workflowInput, marthaDAO), Duration.Inf)
                  case (key, Failure(regret)) => key -> SubmissionValidationValue(None, Some(regret.getMessage), input.workflowInput.localName.value)
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
    val inputs = for ((fqn: FullyQualifiedName, wfInput: InputDefinition) <- workflow.inputs) yield fqn.toString -> nothing
    val outputs = workflow.outputs map (o => o.locallyQualifiedName(workflow) -> nothing)
    MethodConfiguration("namespace", "name", "rootEntityType", Map(), inputs.toMap, outputs.toMap, methodRepoMethod)
  }

  def getMethodInputsOutputs(wdl: String): Try[MethodInputsOutputs] = parseWDL(wdl) map { workflow =>
    MethodInputsOutputs(
      (workflow.inputs map { case (fqn: FullyQualifiedName, wfInput: InputDefinition) => model.MethodInput(fqn, wfInput.womType.toDisplayString, wfInput.optional) }).toSeq,
      workflow.outputs.map(o => MethodOutput(o.locallyQualifiedName(workflow), o.womType.toDisplayString)))
  }
}
