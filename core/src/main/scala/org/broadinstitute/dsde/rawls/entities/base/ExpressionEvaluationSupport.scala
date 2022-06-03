package org.broadinstitute.dsde.rawls.entities.base

import cromwell.client.model.ToolInputParameter
import cromwell.client.model.ValueType.TypeNameEnum
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationSupport.EntityName
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.MethodInput
import org.broadinstitute.dsde.rawls.model.{AttributeNull, AttributeNumber, AttributeString, AttributeValue, AttributeValueEmptyList, AttributeValueList, AttributeValueRawJson, SubmissionValidationEntityInputs, SubmissionValidationValue}
import spray.json.JsArray

import scala.util.{Failure, Success, Try}

object ExpressionEvaluationSupport {
  /*
    These type aliases are to help differentiate between the Entity Name and the Lookup expressions in return types.
    Since both of them are String, it becomes difficult to understand what is being referenced where.
    */
  type EntityName = String
  type LookupExpression = String
  type ExpressionAndResult = (LookupExpression, Map[EntityName, Try[Iterable[AttributeValue]]])
}

trait ExpressionEvaluationSupport {
  protected def createSubmissionValidationEntityInputs(valuesByEntity: Map[EntityName, Seq[SubmissionValidationValue]]): Stream[SubmissionValidationEntityInputs] = {
    valuesByEntity.map({ case (entityName, values) => SubmissionValidationEntityInputs(entityName, values.toSet) }).toStream
  }

  protected def convertToSubmissionValidationValues(attributeMap: Map[EntityName, Try[Iterable[AttributeValue]]], input: MethodInput): Seq[(EntityName, SubmissionValidationValue)] = {
    attributeMap.map {
      case (key, Success(attrSeq)) => key -> unpackResult(attrSeq.toSeq, input.workflowInput)
      case (key, Failure(regret)) => key -> SubmissionValidationValue(None, Some(regret.getMessage), input.workflowInput.getName)
    }.toSeq
  }

  private def unpackResult(mcSequence: Iterable[AttributeValue], wfInput: ToolInputParameter): SubmissionValidationValue = {
    val rawValue = wfInput.getValueType.getTypeName match {
      case TypeNameEnum.ARRAY => getArrayResult(wfInput.getName, mcSequence)
      case TypeNameEnum.OPTIONAL  => if (wfInput.getValueType.getOptionalType.getTypeName == TypeNameEnum.ARRAY)
        getArrayResult(wfInput.getName, mcSequence)
      else getSingleResult(wfInput.getName, mcSequence, wfInput.getOptional) //send optional-arrays down the same codepath as arrays
      case _ => getSingleResult(wfInput.getName, mcSequence, wfInput.getOptional)
    }
    // cast numbers to strings if the input expects a string
    // TODO: handle optional inputs
    // TODO: handle booleans
    // TODO: handle arrays
    if (wfInput.getValueType.getTypeName == TypeNameEnum.STRING) {
      rawValue.value match {
        case Some(n:AttributeNumber) => rawValue.copy(value = Some(AttributeString(n.value.toString())))
        case _ => rawValue
      }
    } else {
      rawValue
    }
  }

  private val emptyResultError = "Expected single value for workflow input, but evaluated result set was empty"
  private val multipleResultError  = "Expected single value for workflow input, but evaluated result set had multiple values"

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
}
