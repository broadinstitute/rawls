package org.broadinstitute.dsde.rawls.expressions

import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, EntityRecord, ReadWriteAction}
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.{AntlrExtendedJSONParser, LookupNodeFinderVisitor}
import org.broadinstitute.dsde.rawls.model.{AttributeBoolean, AttributeNull, AttributeNumber, AttributeString, AttributeValue, AttributeValueRawJson}

import scala.concurrent.ExecutionContext
import scala.util.Try

// a thin abstraction layer over SlickExpressionEvaluator

object ExpressionEvaluator {
  def withNewExpressionEvaluator[R](parser: DataAccess, rootEntities: Option[Seq[EntityRecord]])
                                   (op: ExpressionEvaluator => ReadWriteAction[R])
                                   (implicit executionContext: ExecutionContext): ReadWriteAction[R] = {

    SlickExpressionEvaluator.withNewExpressionEvaluator(parser, rootEntities) { slickEvaluator =>
      op(new ExpressionEvaluator(slickEvaluator, slickEvaluator.rootEntities))
    }
  }

  def withNewExpressionEvaluator[R](parser: DataAccess, workspaceContext: SlickWorkspaceContext, rootType: String, rootName: String)
                                   (op: ExpressionEvaluator => ReadWriteAction[R])
                                   (implicit executionContext: ExecutionContext): ReadWriteAction[R] = {

    SlickExpressionEvaluator.withNewExpressionEvaluator(parser, workspaceContext, rootType, rootName) { slickEvaluator =>
      op(new ExpressionEvaluator(slickEvaluator, slickEvaluator.rootEntities))
    }
  }
}

class ExpressionEvaluator(slickEvaluator: SlickExpressionEvaluator, val rootEntities: Option[Seq[EntityRecord]]) {
  def evalFinalAttribute(workspaceContext: SlickWorkspaceContext, expression: String)
                        (implicit executionContext: ExecutionContext) : ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]] = {
    import slickEvaluator.parser.driver.api._

    /*
      Use the original JsonExpressionEvaluator to evaluate expressions that do not contain lookup nodes
      i.e. attribute reference expressions
     */
    def evaluateLiteralExpression(): ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]] = {
      val evaluatedValue: Try[Iterable[AttributeValue]] = JsonExpressionEvaluator.evaluate(expression)

      DBIO.successful(rootEntities match {
        case Some(entities) => (entities map { entityRec: EntityRecord =>
          entityRec.name -> evaluatedValue
        }).toMap
        case None => Map("" -> evaluatedValue)
      })
    }

    def unpackSlickEvaluatedOutput(value: AttributeValue): String = {
      value match {
        case AttributeNull => "null"
        case AttributeString(v) => s""""$v""""
        case AttributeNumber(v) => v.toString
        case AttributeBoolean(v) => v.toString
        case AttributeValueRawJson(v) => v.toString
      }
    }

    def evaluateAttrExpressions(lookupNodes: Set[String]): ReadWriteAction[Seq[(String, Map[String, Try[Iterable[AttributeValue]]])]] = {
      // evaluate all attribute reference expressions
      val evaluatedLookupSeq: Set[ReadWriteAction[(String, Map[String, Try[Iterable[AttributeValue]]])]] = lookupNodes.map { ref =>
        slickEvaluator.evalFinalAttribute(workspaceContext, ref).map((ref, _))
      }

      DBIO.sequence(evaluatedLookupSeq.toSeq)
    }

    /*
      - parse expression using ANTLR parser
      - somehow find out the all the expressions in JSON that are attribute expression
      - for each attribute expression call slickEvaluator.evalFinalAttribute() and gather the results by running the DBIO actions
      - rebuild the JSON object (!) by replacing that value corresponding to expression in it

      so for expression: {"reference": {"bamFile":this.attrRef}} we need to send {"reference": {"bamFile":"gs://abc/123"}} to Cromwell

      or for expression: ["abc", "123", this.ref] -> ["abc", "123", "gs://abc/123"]
     */

    // ------------------- RECURSIVE APPROACH  ---------------------
    // parse expression using ANTLR ExtendedJSON parser
    val extendedJsonParser = AntlrExtendedJSONParser.getParser(expression)
    val parsedTree = extendedJsonParser.value()

    val lookupVisitor = new LookupNodeFinderVisitor()

//    val lookupNodes: Set[String] = AntlrExtendedJSONParser.findLookupNode(parsedTree)
    val lookupNodes: Set[String] = lookupVisitor.visit(parsedTree)


    if (lookupNodes.isEmpty) evaluateLiteralExpression()
    else {
      /*
    [1, 2, this.bam]

    Set[Map{10019 -> "gs://abc", 10020 -> "gs://def"}]

    -> [1, 2, "gs://abc"]
    -> [1, 2, "gs://def"]

      [
        ("this.bam", Map(10019 -> T[L["gs://abc"]], 10020 -> T[L["gs://abc"]]))
        ("this.index", Map(10019 -> T[L[123]], 10020 -> T[L[345]]))
       ]

       Seq((10019, ("this.bam", "gs://abc")), (10019, ("this.index", 123))

       Map(
       (10019 -> Seq(("this.bam", "gs://abc")))
       )
     */

      // evaluate all attribute reference expressions
      val singleAction: ReadWriteAction[Seq[(String, Map[String, Try[Iterable[AttributeValue]]])]] = evaluateAttrExpressions(lookupNodes)

      val evaluatedExprAction: ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]] = singleAction.map { seqOfExprToSlickEvaluatedMap =>
        val seqOfEntityToLookupExprAndValueMap: Seq[(String, (String, Try[String]))] = seqOfExprToSlickEvaluatedMap.flatMap {
          case (lookupExpr, evaluatedAttrValMap) =>
            // returns  Map[String, (String, Try[String])] -> map of (entityName -> (lookup expr, evaluatedVal))
            val abc = evaluatedAttrValMap.map {
              case (entityName, attrValTry) =>
                val attrRefEvaluatedValueTry: Try[String] = attrValTry.map {
                  case Seq() => "null"
                  case Seq(single) => unpackSlickEvaluatedOutput(single)
                  case multiple =>
                    val unpackedValList = multiple.map(unpackSlickEvaluatedOutput).mkString(",")
                    s"[$unpackedValList]"
                }

                entityName -> (lookupExpr, attrRefEvaluatedValueTry)
            }
            abc
        }

        val mapOfEntityToSeqOfExprAndValue: Map[String, Seq[(String, Try[String])]] = seqOfEntityToLookupExprAndValueMap.groupBy(_._1).map { tuple =>
          tuple._1 -> tuple._2.map(_._2)
        }

//        val abc: Map[String, Try[Map[String, String]]] =  mapOfEntityToSeqOfExprAndValue.map {
//          case (key, value) => key -> Try(value.toMap.mapValues(_.get)) // TODO: clean this up
//        }
//
//        // run the visitor class to form the final expression
//        val deff: Map[String, Try[JsValue]] = abc.mapValues { lookupMapTry =>
//          val entityReplacerParser = AntlrExtendedJSONParser.getParser(expression)
//
//          lookupMapTry.flatMap {lookupMap =>
//            val visitor = new EntityReplacingVisitor(lookupMap)
//
//            Try(entityReplacerParser.value()).map(visitor.visit)
//          }
//        }
//
//        val ghi: Map[String, Try[Iterable[AttributeValue]]] = deff.mapValues(JsonExpressionEvaluator.evaluate)

        // TODO: this might not be the best way to replace values because if expr: [1, 2, "this.bam", this.bam] we
        //  need to only replace the 4th element and not the third one
        val mapOfEntityToEvaluatedExpr: Map[String, String] = mapOfEntityToSeqOfExprAndValue.map {
          case (entityName, lookupToValueMap) => {
//            lookupToValueMap.foldLeft(expression)((a, b) => b._2.map(x => a.replaceAll(b._1, x)))

            var newExpression = expression // TODO: there should be a better way! Think! Maybe fold?
            lookupToValueMap.foreach {
              case (lookupExpr, attrTryVal) => {
                attrTryVal.map { x =>
                  newExpression = newExpression.replaceAll(lookupExpr, x)
                }
              }
            }

            entityName -> newExpression
          }
        }

        mapOfEntityToEvaluatedExpr.map {
          case (entityName, newExpression) => entityName -> JsonExpressionEvaluator.evaluate(newExpression)
        }
      }

      evaluatedExprAction
    }




//    val evaluatedNodeMap: Set[(String, Map[String, Try[Iterable[AttributeValue]]])] =
//      lookupNodes.map{ reference =>
//        val evaluatedValue: Map[String, Try[Iterable[AttributeValue]]] = Await.result(
//          dataSource.inTransaction(_ => slickEvaluator.evalFinalAttribute(workspaceContext, reference)),
//          5.minutes
//        )
//
//        (reference, evaluatedValue)
//      }
//
//    println(evaluatedNodeMap)
//
//    val head = lookupNodes.head


//    val evaluated = slickEvaluator.evalFinalAttribute(workspaceContext, head)
//
//    val entityQuery = dataSource.dataAccess.entityQuery
//
//    val action = entityQuery.findEntityByName(workspaceContext.workspaceId, "SampleSet", "sset1")
//
//
//
//      //.result flatMap { entityRec =>
//      ExpressionEvaluator.withNewExpressionEvaluator(this, Some(entityRec)) { evaluator =>
//
//      }
//
//    Await.result(
//      dataSource.inTransaction( _ => evaluated)
//      , 5.minutes
//    )




//    val result = Await.result(
//              dataSource.inTransaction(_ => slickEvaluator.evalFinalAttribute(workspaceContext, head)),
//              5.minutes
//    )
//
//    println(result)


    // ------------------- END RECURSIVE APPROACH  ---------------------


    // ------------------- VISITOR PATTERN ---------------------
//    val extendedJsonParser1 = AntlrExtendedJSONParser.getParser(expression)
//    val visitor = new ExtendedJSONVisitorEvaluateImpl(expression, slickEvaluator, workspaceContext)
//
//
//    val abc = extendedJsonParser1.value()
//
//    val duff: ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]] = visitor.visit(abc)
//
//    duff

    // ------------------- END VISITOR PATTERN ---------------------


    // ------------------- ORIGINAL ---------------------


//    val xa1: ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]] = JsonExpressionEvaluator.evaluate(expression) match {
//      //if the expression evals as JSON, it evaluates to the same thing for every entity, so build that map here
//      case Success(parsed) => val xa2: ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]] = DBIO.successful(rootEntities match {
//        case Some(entities) => (entities map { entityRec: EntityRecord =>
//          entityRec.name -> Success(parsed)
//        }).toMap
//        case None => Map("" -> Success(parsed))
//      })
//        xa2
//
//      case Failure(_) => slickEvaluator.evalFinalAttribute(workspaceContext, expression)
//    }

    // ------------------- END ORIGINAL ---------------------
  }

  def evalFinalEntity(workspaceContext: SlickWorkspaceContext, expression:String): ReadWriteAction[Iterable[EntityRecord]] = {
    //entities have to be proper expressions, not JSON-y
    slickEvaluator.evalFinalEntity(workspaceContext, expression)
  }
}
