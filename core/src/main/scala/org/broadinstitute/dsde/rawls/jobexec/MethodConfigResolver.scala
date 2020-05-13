package org.broadinstitute.dsde.rawls.jobexec
import cromwell.client.model.ValueType.TypeNameEnum
import cromwell.client.model.{ToolInputParameter, WorkflowDescription}
import org.broadinstitute.dsde.rawls.dataaccess.{SlickDataSource, SlickWorkspaceContext}
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.expressions.ExpressionEvaluator
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.{GatherInputsResult, MethodInput}
import org.broadinstitute.dsde.rawls.jobexec.wdlparsing.WDLParser
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.CollectionUtils
import org.broadinstitute.dsde.rawls.{RawlsException, model}
import spray.json._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

class MethodConfigResolver(wdlParser: WDLParser) {
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
    val notNull: Seq[AttributeValue] = seq.filter(v => v != null && v != AttributeNull).toSeq
    val attr = notNull match {
      case Nil => Option(AttributeValueEmptyList)
      //GAWB-2509: don't pack single-elem RawJson array results into another layer of array
      //NOTE: This works, except for the following situation: a participant with a RawJson double-array attribute, in a single-element participant set.
      // Evaluating this.participants.raw_json on the pset will incorrectly hit this case and return a 2D array when it should return a 3D array.
      // The true fix for this is to look into why the slick expression evaluator wraps deserialized AttributeValues in a Seq, and instead
      // return the proper result type, removing the need to infer whether it's a scalar or array type from the WDL input.
      case AttributeValueRawJson(JsArray(_)) +: Seq() => Option(notNull.head)
      case _ => Option(AttributeValueList(notNull))
    }
    SubmissionValidationValue(attr, None, inputName)
  }

  private def unpackResult(mcSequence: Iterable[AttributeValue], wfInput: ToolInputParameter): SubmissionValidationValue = wfInput.getValueType.getTypeName match {
    case TypeNameEnum.ARRAY => getArrayResult(wfInput.getName, mcSequence)
    case TypeNameEnum.OPTIONAL  => if (wfInput.getValueType.getOptionalType.getTypeName == TypeNameEnum.ARRAY)
                                     getArrayResult(wfInput.getName, mcSequence)
                                   else getSingleResult(wfInput.getName, mcSequence, wfInput.getOptional) //send optional-arrays down the same codepath as arrays
    case _ => getSingleResult(wfInput.getName, mcSequence, wfInput.getOptional)
  }


  def parseWDL(userInfo: UserInfo, wdl: WDL)(implicit executionContext: ExecutionContext): Try[WorkflowDescription] = {
    wdlParser.parse(userInfo, wdl)
  }

  def gatherInputs(userInfo: UserInfo, methodConfig: MethodConfiguration, wdl: WDL)(implicit executionContext: ExecutionContext): Try[GatherInputsResult] = parseWDL(userInfo, wdl) map { parsedWdlWorkflow =>
    def isAttributeEmpty(fqn: String): Boolean = {
      methodConfig.inputs.get(fqn) match {
        case Some(AttributeString(value)) => value.isEmpty
        case _ => throw new RawlsException(s"MethodConfiguration ${methodConfig.namespace}/${methodConfig.name} input ${fqn} value is unavailable")
      }
    }

    val wdlInputs = parsedWdlWorkflow.getInputs.asScala

    /* ok, so there are:
        - inputs that the WDL requires and the MC defines: ok - definedInputs
        - inputs that the WDL says are optional but the MC defines anyway: ok - contained in definedInputs
        - inputs that the WDL requires and the MC does not define: bad - missingRequired
        - inputs that the WDL says are optional and the MC doesn't define: ok - emptyOptionals
        - inputs that the WDL has no idea about but the MC defines anyway: bad - extraInputs
     */
    val (definedInputs, undefinedInputs) = wdlInputs.partition { input => methodConfig.inputs.contains(input.getName) && !isAttributeEmpty(input.getName) }
    val (emptyOptionals, missingRequired) = undefinedInputs.partition { input => input.getOptional }
    val extraInputs = methodConfig.inputs.filterNot { case (name, expression) => wdlInputs.map(_.getName).contains(name) }


    GatherInputsResult(
      definedInputs.map  { input => MethodInput(input, methodConfig.inputs(input.getName).value) }.toSet,
      emptyOptionals.map { input => MethodInput(input, methodConfig.inputs.getOrElse(input.getName, AttributeString("")).value ) }.toSet,
      missingRequired.map(_.getName).toSet,
      extraInputs.keys.toSet)
  }

  def evaluateInputExpressions(workspaceContext: SlickWorkspaceContext,
                               inputs: Set[MethodInput],
                               entities: Option[Seq[EntityRecord]],
                               dataAccess: DataAccess,
                               dataSource: SlickDataSource)(implicit executionContext: ExecutionContext): ReadWriteAction[Map[String, Seq[SubmissionValidationValue]]] = {
    import dataAccess.driver.api._

    val entityNames = entities match {
      case Some(recs) => recs.map(_.name)
      case None => Seq("")
    }

    if( inputs.isEmpty ) {
      //no inputs to evaluate = just return an empty map back!
      DBIO.successful(entityNames.map( _ -> Seq.empty[SubmissionValidationValue] ).toMap)
    } else {
      ExpressionEvaluator.withNewExpressionEvaluator(dataAccess, entities) { evaluator =>
        //Evaluate the results per input and return a seq of DBIO[ Map(entity -> value) ], one per input
        val resultsByInput = inputs.toSeq.map { input =>
          evaluator.evalFinalAttribute(workspaceContext, input.expression).asTry.map { tryAttribsByEntity =>
            val validationValuesByEntity: Seq[(String, SubmissionValidationValue)] = tryAttribsByEntity match {
              case Failure(regret) =>
                //The DBIOAction failed - this input expression was not evaluated. Make an error for each entity.
                entityNames.map((_, SubmissionValidationValue(None, Some(regret.getMessage), input.workflowInput.getName)))
              case Success(attributeMap) =>
                //The expression was evaluated, but that doesn't mean we got results...
                attributeMap.map {
                  case (key, Success(attrSeq)) => key -> unpackResult(attrSeq.toSeq, input.workflowInput)
                  case (key, Failure(regret)) => key -> SubmissionValidationValue(None, Some(regret.getMessage), input.workflowInput.getName)
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

  def getMethodInputsOutputs(userInfo: UserInfo, wdl: WDL)(implicit executionContext: ExecutionContext): Try[MethodInputsOutputs] = parseWDL(userInfo, wdl) map { workflowDescription =>
    if (workflowDescription.getValid) {
      val inputs = workflowDescription.getInputs.asScala.toList map { input =>
        model.MethodInput(input.getName, input.getTypeDisplayName.replaceAll("\\n", ""), input.getOptional)
      }
      val outputs = workflowDescription.getOutputs.asScala.toList map { output =>
        model.MethodOutput(output.getName, output.getTypeDisplayName.replace("\\n", ""))
    }
    MethodInputsOutputs(inputs, outputs)
  } else throw new RawlsException(workflowDescription.getErrors.asScala.mkString("\n"))
  }

  def toMethodConfiguration(userInfo: UserInfo, wdl: WDL, methodRepoMethod: MethodRepoMethod)(implicit executionContext: ExecutionContext): Try[MethodConfiguration] = {
    val empty = AttributeString("")
    getMethodInputsOutputs(userInfo, wdl) map { io =>
      val inputs = io.inputs map { _.name -> empty }
      val outputs = io.outputs map { _.name -> empty }
      MethodConfiguration("namespace", "name", Some("rootEntityType"), Some(Map.empty[String, AttributeString]), inputs.toMap, outputs.toMap, methodRepoMethod)
    }
  }

}

object MethodConfigResolver {
  case class MethodInput(workflowInput: ToolInputParameter, expression: String)

  case class GatherInputsResult(processableInputs: Set[MethodInput], emptyOptionalInputs: Set[MethodInput], missingInputs: Set[String], extraInputs: Set[String])

}
