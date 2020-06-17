package org.broadinstitute.dsde.rawls.expressions

import org.antlr.v4.runtime.tree.ParseTree
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, EntityRecord, ReadWriteAction}
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.{AntlrExtendedJSONParser, LocalEvaluateToEntityVisitor, LocalEvaluateToAttributeVisitor, ReconstructExpressionVisitor}
import org.broadinstitute.dsde.rawls.model.{AttributeBoolean, AttributeNull, AttributeNumber, AttributeString, AttributeValue, AttributeValueRawJson, Workspace}
import spray.json.{JsArray, JsBoolean, JsNull, JsNumber, JsString, JsValue}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

// a thin abstraction layer over SlickExpressionEvaluator

object ExpressionEvaluator {
  def withNewExpressionEvaluator[R](parser: DataAccess, rootEntities: Option[Seq[EntityRecord]])
                                   (op: ExpressionEvaluator => ReadWriteAction[R])
                                   (implicit executionContext: ExecutionContext): ReadWriteAction[R] = {

    SlickExpressionEvaluator.withNewExpressionEvaluator(parser, rootEntities) { slickEvaluator =>
      op(new ExpressionEvaluator(slickEvaluator, slickEvaluator.rootEntities))
    }
  }

  def withNewExpressionEvaluator[R](parser: DataAccess, workspaceContext: Workspace, rootType: String, rootName: String)
                                   (op: ExpressionEvaluator => ReadWriteAction[R])
                                   (implicit executionContext: ExecutionContext): ReadWriteAction[R] = {

    SlickExpressionEvaluator.withNewExpressionEvaluator(parser, workspaceContext, rootType, rootName) { slickEvaluator =>
      op(new ExpressionEvaluator(slickEvaluator, slickEvaluator.rootEntities))
    }
  }
}

class ExpressionEvaluator(slickEvaluator: SlickExpressionEvaluator, val rootEntities: Option[Seq[EntityRecord]]) {
  /**
  These type aliases are to help differentiate between the Entity Name and the Lookup expressions in return types
     in below functions. Since both of them are String, it becomes difficult to understand what is being referenced where.
    */
  private type EntityName = String
  private type LookupExpression = String // attribute reference expression

  /**
    * Converts AttributeValue to JsValue (JsValue instead of String so as to preserve the evaluated value structure)
    */
  def unpackSlickEvaluatedOutput(value: AttributeValue): JsValue = {
    value match {
      case AttributeNull => JsNull
      case AttributeString(v) => JsString(v)
      case AttributeNumber(v) => JsNumber(v)
      case AttributeBoolean(v) => JsBoolean(v)
      case AttributeValueRawJson(v) => v
    }
  }

  /**
  The overall approach is:
        - Parse the input expression using ANTLR Extended JSON parser
        - Visit the parsed tree to find all the look up nodes (i.e. attribute reference expressions)
        - If there are no look up nodes, evaluate the input expression using the JSONEvaluator
        - If there are look up nodes:
            - for each look up node, evaluate the attribute reference through SlickEvaluator
            - through a series of transformations, generate a Map of entity name to Map of lookup expressions and their
              evaluated value for that entity
            - for each entity, substitute the evaluated values of attribute references back into the input expression
              by visiting the parsed tree of input expression
            - for each entity, pass the reconstructed input expression to JSONEvaluator to parse the expression into
              AttributeValue
    To help understand the approach if their are attribute references present, we will follow the below example roughly:
      expression = "{"exampleRef1":this.bam, "exampleIndex":this.index}"
      rootEntities = Seq(101, 102) (here we assume the entity name is 101 for Entity Record 1 and 102 for Entity record 2)

    The final output will be:
    ReadWriteAction(Map(
          "101" -> Try(Seq(AttributeValueRawJson("{"exampleRef1":"gs://abc", "exampleIndex":123}"))),
          "102" -> Try(Seq(AttributeValueRawJson("{"exampleRef1":"gs://def", "exampleIndex":456}")))
        )
    )
    */
  def evalFinalAttribute(workspaceContext: Workspace, expression: String)(implicit executionContext: ExecutionContext): ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]] = {
    import slickEvaluator.dataAccess.driver.api._

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
    def unpackAndTransformEvaluatedOutput(seqOfTuple: Seq[(LookupExpression, Map[EntityName, Try[Iterable[AttributeValue]]])])
    : Seq[(EntityName, (LookupExpression, Try[JsValue]))] = {
      seqOfTuple.flatMap {
        case (lookupExpr, slickEvaluatedAttrValueMap) =>
          // returns  Map[EntityName, (LookupExpression, Try[JsValue])]
          slickEvaluatedAttrValueMap.map {
            case (entityName, attrValueTry) =>
              val unpackedEvaluatedValueTry: Try[JsValue] = attrValueTry.map {
                case Seq() => JsNull
                case Seq(single) => unpackSlickEvaluatedOutput(single)
                case multiple => JsArray(multiple.map(unpackSlickEvaluatedOutput).toVector)
              }

              entityName -> (lookupExpr, unpackedEvaluatedValueTry)
          }
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
    def groupByEntityName(seqOfEntityToLookupExprAndValue: Seq[(EntityName, (LookupExpression, Try[JsValue]))])
    : Map[EntityName, Seq[(LookupExpression, Try[JsValue])]] = {
      seqOfEntityToLookupExprAndValue.groupBy(_._1).map { tuple =>
        tuple._1 -> tuple._2.map(_._2)
      }
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
    def convertEvaluatedExprSeqToMap(mapOfEntityToSeqOfEvaluatedExpr: Map[EntityName, Seq[(LookupExpression, Try[JsValue])]])
    : Map[EntityName, Try[Map[LookupExpression, JsValue]]] = {
      mapOfEntityToSeqOfEvaluatedExpr.map {
        case (entityName, value) =>
          val evaluatedExprMapTry = Try(value.toMap.map {
            case (lookupExpr, jsValueTry) => lookupExpr -> jsValueTry.get
          })

          entityName -> evaluatedExprMapTry
      }
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
    def reconstructInputExprForEachEntity(mapOfEntityToEvaluatedExprMap: Map[EntityName, Try[Map[LookupExpression, JsValue]]],
                                          parsedTree: ParseTree): Map[EntityName, Try[JsValue]] = {
      mapOfEntityToEvaluatedExprMap.map {
        case (entityName, evaluatedLookupMapTry) =>
          val inputExprWithEvaluatedRef = evaluatedLookupMapTry.flatMap { lookupMap =>
            val visitor = new ReconstructExpressionVisitor(lookupMap)
            Try(visitor.visit(parsedTree))
          }

          entityName -> inputExprWithEvaluatedRef
      }
    }

    /*
      This method transforms the seq of look up expressions and their evaluated value, reconstructs the original input expression
      after replacing the evaluated values of attribute expressions and parses it again using the JsonExpressionEvaluator.
      For our example:
        input: seqOfLookupExprOp = Seq(
          ("this.bam", Map("101" -> Try(Seq(AttributeString("gs://abc"))), "102" -> Try(Seq(AttributeString("gs://def"))))),
          ("this.index", Map("101" -> Try(Seq(AttributeNumber(123))), "102" -> Try(Seq(AttributeNumber(456)))))
        ) and parsed tree
        output = Map(
          "101" -> Try(Seq(AttributeValueRawJson("{"exampleRef1":"gs://abc", "exampleIndex":123}"))),
          "102" -> Try(Seq(AttributeValueRawJson("{"exampleRef1":"gs://def", "exampleIndex":456}")))
        )
     */
    def transformAndParseExpr(seqOfLookupExprOp: Seq[(LookupExpression, Map[EntityName, Try[Iterable[AttributeValue]]])],
                              parsedTree: ParseTree): Map[EntityName, Try[Iterable[AttributeValue]]] = {
      // unpack the evaluated AttributeValue to JsValue and transform it into sequence of tuples with entity name as key
      val seqOfEntityToLookupExprAndValue: Seq[(EntityName, (LookupExpression, Try[JsValue]))] = unpackAndTransformEvaluatedOutput(seqOfLookupExprOp)

      // group the tuples by entity name and convert it to a Map
      val mapOfEntityToSeqOfEvaluatedExpr: Map[EntityName, Seq[(LookupExpression, Try[JsValue])]] = groupByEntityName(seqOfEntityToLookupExprAndValue)

      // convert the values in the Map, which are sequence of expr and their value to a Map
      val mapOfEntityToEvaluatedExprMap: Map[EntityName, Try[Map[LookupExpression, JsValue]]] = convertEvaluatedExprSeqToMap(mapOfEntityToSeqOfEvaluatedExpr)

      // replace the value for evaluated attribute references in the input expression for each entity name
      val mapOfEntityToInputExpr: Map[EntityName, Try[JsValue]] = reconstructInputExprForEachEntity(mapOfEntityToEvaluatedExprMap, parsedTree)

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
      mapOfEntityToInputExpr.map { case (entityName, exprTry) => entityName -> JsonExpressionEvaluator.evaluate(exprTry) }

    }

    /*
      Use the original JsonExpressionEvaluator to evaluate expressions that do not contain lookup nodes
      i.e. attribute reference expressions
     */
    def evaluateLiteralExpression(): ReadWriteAction[Map[EntityName, Try[Iterable[AttributeValue]]]] = {
      val evaluatedValue: Try[Iterable[AttributeValue]] = JsonExpressionEvaluator.evaluate(expression)

      DBIO.successful(rootEntities match {
        case Some(entities) => (entities map { entityRec: EntityRecord =>
          entityRec.name -> evaluatedValue
        }).toMap
        case None => Map("" -> evaluatedValue)
      })
    }


    // parse expression using ANTLR ExtendedJSON parser
    val extendedJsonParser = AntlrExtendedJSONParser.getParser(expression)
    val localFinalAttributeEvaluationVisitor = new LocalEvaluateToAttributeVisitor(workspaceContext, slickEvaluator)

    Try(extendedJsonParser.root()) match {
      case Success(parsedTree) =>
        /*
          Evaluate all attribute reference expressions if any.
          For our example:
            lookupNodes = Set("this.bam", "this.index")
        */

        localFinalAttributeEvaluationVisitor.visit(parsedTree).flatMap {
          case Seq() => evaluateLiteralExpression()
          case result => DBIO.successful(transformAndParseExpr(result, parsedTree))
        }

      case Failure(regrets) => DBIO.failed(regrets)
    }
  }

  def evalFinalEntity(workspaceContext: Workspace, expression: String)
                     (implicit executionContext: ExecutionContext): ReadWriteAction[Iterable[EntityRecord]] = {
    import slickEvaluator.dataAccess.driver.api._

    val extendedJSONParser = AntlrExtendedJSONParser.getParser(expression)
    val localFinalEntityEvaluationVisitor = new LocalEvaluateToEntityVisitor(workspaceContext, slickEvaluator)

    Try(extendedJSONParser.root()) match {
      case Success(parsedTree) => localFinalEntityEvaluationVisitor.visit(parsedTree)
      case Failure(regrets) => DBIO.failed(regrets)
    }
  }
}
