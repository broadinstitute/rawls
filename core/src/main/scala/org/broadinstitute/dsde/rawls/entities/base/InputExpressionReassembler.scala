package org.broadinstitute.dsde.rawls.entities.base

import cromwell.client.model.ValueType.TypeNameEnum
import cromwell.client.model.ValueTypeObjectFieldTypes
import org.antlr.v4.runtime.tree.ParseTree
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationSupport.{
  EntityName,
  ExpressionAndResult,
  LookupExpression
}
import org.broadinstitute.dsde.rawls.expressions.JsonExpressionEvaluator
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.ReconstructExpressionVisitor
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.MethodInput
import org.broadinstitute.dsde.rawls.model.{
  AttributeBoolean,
  AttributeNull,
  AttributeNumber,
  AttributeString,
  AttributeValue,
  AttributeValueRawJson
}
import spray.json.{JsArray, JsBoolean, JsNull, JsNumber, JsObject, JsString, JsValue}

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.{Success, Try}

object InputExpressionReassembler {

  /**
    * This method transforms lookupExpressionAndResults to reconstructs the original input expression
    * by replacing the evaluated values of lookup expressions in the parsedInputExpression then
    * parses the result using the JsonExpressionEvaluator to convert to AttributeValue.
    * For our example:
    *  input: lookupExpressionAndResults = Seq(
    *    ("this.bam", Map("101" -> Try(Seq(AttributeString("gs://abc"))), "102" -> Try(Seq(AttributeString("gs://def"))))),
    *    ("this.index", Map("101" -> Try(Seq(AttributeNumber(123))), "102" -> Try(Seq(AttributeNumber(456)))))
    *  ) and parsed tree
    * output = Map(
    *   "101" -> Try(Seq(AttributeValueRawJson("{"exampleRef1":"gs://abc", "exampleIndex":123}"))),
    *   "102" -> Try(Seq(AttributeValueRawJson("{"exampleRef1":"gs://def", "exampleIndex":456}")))
    * )
    *
    * @param lookupExpressionAndResults the collection of all lookup expressions referenced in the given input expression
    *                                   and the results for each entity
    * @param parsedInputExpression the input expression as parsed by ANTLR
    * @param rootEntityNames the names of all the root entities being evaluated
    * @return key = the name of each root entity, value = the final input value
    */
  def constructFinalInputValues(lookupExpressionAndResults: Seq[ExpressionAndResult],
                                parsedInputExpression: ParseTree,
                                rootEntityNames: Option[Seq[EntityName]],
                                input: Option[MethodInput]
  ): Map[EntityName, Try[Iterable[AttributeValue]]] = {
    // unpack the evaluated AttributeValue to JsValue and transform it into sequence of tuples with entity name as key
    val seqOfEntityToLookupExprAndValue: Seq[(EntityName, (LookupExpression, Try[JsValue]))] =
      unpackAndTransformEvaluatedOutput(lookupExpressionAndResults)

    // group the tuples by entity name and convert it to a Map
    val mapOfEntityToSeqOfEvaluatedExpr: Map[EntityName, Seq[(LookupExpression, Try[JsValue])]] = groupByEntityName(
      seqOfEntityToLookupExprAndValue
    )

    // convert the values in the Map, which are sequence of expr and their value to a Map
    val mapOfEntityToEvaluatedExprMap: Map[EntityName, Try[Map[LookupExpression, JsValue]]] =
      convertEvaluatedExprSeqToMap(mapOfEntityToSeqOfEvaluatedExpr)

    // replace the value for evaluated attribute references in the input expression for each entity name
    val mapOfEntityToInputExpr: Map[EntityName, Try[JsValue]] =
      reconstructInputExprForEachEntity(mapOfEntityToEvaluatedExprMap, parsedInputExpression, rootEntityNames, input)

    /*
      For each entity name and it's generated expression call JsonExpressionEvaluator to reconstruct the desired return type
      For our example:
      input = Map(
        "101" -> Try(JsObject("{"exampleRef1":"gs://abc", "exampleIndex":123}")),
        "102" -> Try(JsObject("{"exampleRef1":"gs://def", "exampleIndex":456}"))
      )
      output = Map(
        "101" -> Try(Seq(AttributeValueRawJson("{"exampleRef1":"gs://abc", "exampleIndex":123}"))),
        "102" -> Try(Seq(AttributeValueRawJson("{"exampleRef1":"gs://def", "exampleIndex":456}")))
      )
     */
    mapOfEntityToInputExpr.map { case (entityName, exprTry) =>
      val exprValue = JsonExpressionEvaluator.evaluate(exprTry)
      entityName -> exprValue
    }

  }

  /**
    * Converts AttributeValue to JsValue (JsValue instead of String so as to preserve the evaluated value structure)
    */
  private def unpackSlickEvaluatedOutput(value: AttributeValue): JsValue =
    value match {
      case AttributeNull            => JsNull
      case AttributeString(v)       => JsString(v)
      case AttributeNumber(v)       => JsNumber(v)
      case AttributeBoolean(v)      => JsBoolean(v)
      case AttributeValueRawJson(v) => v
    }

  /*
        Unpack the evaluated value for each lookup expression and convert it into JsValue. It also converts the
        input Seq to Seq of tuple where key is the entity name and the value itself a tuple of lookup expression and it's
        evaluated value for that entity
        For our example:
          input = Seq(
            ("this.bam", Map("101" -> Try(Seq(AttributeString("gs://abc"))), "102" -> Try(Seq(AttributeString("gs://def"))))),
            ("this.index", Map("101" -> Try(Seq(AttributeNumber(123))), "102" -> Try(Seq(AttributeNumber(456)))))
          )
          output = Seq(
            ("101", ("this.bam", Try(JsString("gs://abc")))),
            ("101", ("this.index", Try(JsNumber(123)))),
            ("102", ("this.bam", Try(JsString("gs://def")))),
            ("102", ("this.index", Try(JsNumber(456))))
          )
   */
  private def unpackAndTransformEvaluatedOutput(
    seqOfTuple: Seq[ExpressionAndResult]
  ): Seq[(EntityName, (LookupExpression, Try[JsValue]))] =
    seqOfTuple.flatMap { case (lookupExpr, slickEvaluatedAttrValueMap) =>
      // returns  Map[EntityName, (LookupExpression, Try[JsValue])]
      slickEvaluatedAttrValueMap.map { case (entityName, attrValueTry) =>
        val unpackedEvaluatedValueTry: Try[JsValue] = attrValueTry.map {
          case Seq()       => JsNull
          case Seq(single) => unpackSlickEvaluatedOutput(single)
          case multiple    => JsArray(multiple.map(unpackSlickEvaluatedOutput).toVector)
        }

        entityName -> (lookupExpr, unpackedEvaluatedValueTry)
      }
    }

  /*
    Group the tuples based on entity name as key and convert it into a Map. The values are tuple of lookup expression
    and its evaluated value
    For our example:
      input = Seq(
        ("101", ("this.bam", Try(JsString("gs://abc")))),
        ("101", ("this.index", Try(JsNumber(123)))),
        ("102", ("this.bam", Try(JsString("gs://def")))),
        ("102", ("this.index", Try(JsNumber(456))))
      )
      output = Map(
        "101" -> Seq(("this.bam", Try(JsString("gs://abc"))), ("this.index", Try(JsNumber(123)))),
        "102" -> Seq(("this.bam", Try(JsString("gs://def"))), ("this.index", Try(JsNumber(456))))
      )
   */
  private def groupByEntityName(
    seqOfEntityToLookupExprAndValue: Seq[(EntityName, (LookupExpression, Try[JsValue]))]
  ): Map[EntityName, Seq[(LookupExpression, Try[JsValue])]] =
    seqOfEntityToLookupExprAndValue.groupBy(_._1).map { tuple =>
      tuple._1 -> tuple._2.map(_._2)
    }

  /*
    Converts the sequence of evaluated lookup expression tuple to Map
    For our example:
      input = Map(
        "101" -> Seq(("this.bam", Try(JsString("gs://abc"))), ("this.index", Try(JsNumber(123)))),
        "102" -> Seq(("this.bam", Try(JsString("gs://def"))), ("this.index", Try(JsNumber(456))))
      )
      output = Map(
        "101" -> Try(Map("this.bam" -> JsString("gs://abc"), "this.index" -> JsNumber(123))),
        "102" -> Try(Map("this.bam" -> JsString("gs://def"), "this.index" -> JsNumber(456)))
      )
   */
  private def convertEvaluatedExprSeqToMap(
    mapOfEntityToSeqOfEvaluatedExpr: Map[EntityName, Seq[(LookupExpression, Try[JsValue])]]
  ): Map[EntityName, Try[Map[LookupExpression, JsValue]]] =
    mapOfEntityToSeqOfEvaluatedExpr.map { case (entityName, value) =>
      val evaluatedExprMapTry = Try(value.toMap.map { case (lookupExpr, jsValueTry) =>
        lookupExpr -> jsValueTry.get
      })

      entityName -> evaluatedExprMapTry
    }

  /*
    For each entity, substitute the evaluated values of attribute references back into the input expression
    using the visitor pattern
    For our example:
      input: mapOfEntityToEvaluatedExprMap = Map(
        "101" -> Try(Map("this.bam" -> JsString("gs://abc"), "this.index" -> JsNumber(123))),
        "102" -> Try(Map("this.bam" -> JsString("gs://def"), "this.index" -> JsNumber(456)))
      )
      output = Map(
        "101" -> Try(JsObject("{"exampleRef1":"gs://abc", "exampleIndex":123}")),
        "102" -> Try(JsObject("{"exampleRef1":"gs://def", "exampleIndex":456}"))
      )
   */
  private def reconstructInputExprForEachEntity(
    mapOfEntityToEvaluatedExprMap: Map[EntityName, Try[Map[LookupExpression, JsValue]]],
    parsedTree: ParseTree,
    rootEntityNames: Option[Seq[EntityName]],
    inputOption: Option[MethodInput]
  ): Map[EntityName, Try[JsValue]] = {

    /* This method is called below and used to re-format inputs that are like JSON objects. For each field in the object, we verify for
       Array type inputs that the evaluated value is also an Array. If it isn't we convert it into Array and updated the evaluated object.
     */
    def updateInputExprValueMapForObjectInput(inputExprWithEvaluatedRef: JsValue,
                                              objectFieldsFromInput: mutable.Seq[ValueTypeObjectFieldTypes]
    ): JsObject = {
      val updatedObject: Map[String, JsValue] = inputExprWithEvaluatedRef.asJsObject.fields.map {
        case (inputName, evaluatedValue) =>
          val inputObjectField: Option[ValueTypeObjectFieldTypes] =
            objectFieldsFromInput.find(x => x.getFieldName == inputName)

          val updatedValue: JsValue = inputObjectField match {
            case Some(objectField) =>
              objectField.getFieldType.getTypeName match {
                case TypeNameEnum.OBJECT =>
                  // if there are nested objects, recursively call this method to reach leaf fields
                  updateInputExprValueMapForObjectInput(evaluatedValue,
                                                        objectField.getFieldType.getObjectFieldTypes.asScala
                  )
                case TypeNameEnum.ARRAY =>
                  evaluatedValue match {
                    case JsNull     => JsArray()
                    case JsArray(_) => evaluatedValue
                    case _          => JsArray(evaluatedValue)
                  }
                case _ => evaluatedValue
              }
            case None => evaluatedValue
          }
          inputName -> updatedValue
      }

      JsObject(updatedObject)
    }

    // when there are no root entities handle as a single root entity with empty string for name
    rootEntityNames
      .getOrElse(Seq(""))
      .map { entityName =>
        // in the case of literal JSON there are no LookupExpressions
        val evaluatedLookupMapTry =
          mapOfEntityToEvaluatedExprMap.getOrElse(entityName, Success(Map.empty[LookupExpression, JsValue]))
        val inputExprWithEvaluatedRef: Try[JsValue] = evaluatedLookupMapTry.map { lookupMap =>
          val visitor = new ReconstructExpressionVisitor(lookupMap)
          visitor.visit(parsedTree)
        }

        /* In SlickExpressionEvaluator, for attribute references that evaluate to AttributeValueList type, the elements inside the list are extracted and returned.
         As a result, for attributes that contain only 1 element, we can no longer identify whether the original value was a list or a single AttributeValue.
         Because of this type erasure, when we are reconstructing object type inputs, List/Array with single element get incorrectly reconstructed as single AttributeValue,
         which leads to Cromwell failing while processing inputs (https://broadworkbench.atlassian.net/browse/BW-678).
         To fix this, once the object type inputs have been reconstructed, we iterate over each field for object input and verify for Array type inputs that the
         evaluated value is also an Array. If it isn't we convert it into Array and updated the evaluated object.
         Note: This approach does not fix the problem if there is single element array inside an array. This would be fixed in https://broadworkbench.atlassian.net/browse/BW-846.
         Fixing this might possible require fixing the type erasure, changes to what AttributeValueList extends and update how we convert JsValue to AttributeValue
         and vice-versa in JsonSupport class.
         */
        val updatedEvaluatedRefMap: Try[JsValue] = inputOption match {
          case Some(input) =>
            input.workflowInput.getValueType.getTypeName match {
              case TypeNameEnum.OBJECT =>
                val objectFields = Option(input.workflowInput.getValueType.getObjectFieldTypes)
                  .map(_.asScala)
                  .getOrElse(mutable.Seq.empty[ValueTypeObjectFieldTypes])
                inputExprWithEvaluatedRef.map(x => updateInputExprValueMapForObjectInput(x, objectFields))
              case _ =>
                inputExprWithEvaluatedRef
            }
          case None => inputExprWithEvaluatedRef
        }
        entityName -> updatedEvaluatedRefMap
      }
      .toMap
  }
}
