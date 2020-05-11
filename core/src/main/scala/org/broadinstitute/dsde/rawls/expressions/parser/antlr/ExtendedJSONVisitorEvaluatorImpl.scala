package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import org.antlr.v4.runtime.misc.ParseCancellationException
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, EntityRecord, ReadWriteAction}
import org.broadinstitute.dsde.rawls.dataaccess.{SlickDataSource, SlickWorkspaceContext}
import org.broadinstitute.dsde.rawls.expressions.SlickExpressionEvaluator
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.ExtendedJSONParser.{LiteralContext, LookupContext}
import org.broadinstitute.dsde.rawls.model.{AttributeBoolean, AttributeNumber, AttributeString, AttributeValue}
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success, Try}

class ExtendedJSONVisitorEvaluatorImpl(expression: String,
                                       slickEvaluator: SlickExpressionEvaluator,
                                       workspaceContext: SlickWorkspaceContext,
                                       dataSource: SlickDataSource,
                                       dataAccess: DataAccess)
                                      (implicit executionContext: ExecutionContext) extends
  ExtendedJSONBaseVisitor[Map[String, Try[Iterable[AttributeValue]]]] {


  override def visitLiteral(ctx: LiteralContext): Map[String, Try[Iterable[AttributeValue]]] = {
    val expression = ctx.getText

    val attributeValTry: Try[Iterable[AttributeValue]] = Try(expression.parseJson) map {
      case JsNull => Seq.empty
      case JsString(s) => Seq(AttributeString(s))
      case JsBoolean(b) => Seq(AttributeBoolean(b))
      case JsNumber(n) => Seq(AttributeNumber(n))
      case x => throw new ParseCancellationException(s"Something went wrong! $x")
    } recover {
      case exception => throw new ParseCancellationException(s"Something went wrong! $exception")
    }

    attributeValTry match {
      case Success(attributeVal) => slickEvaluator.rootEntities match {
        case Some(entities) => (entities map { entityRec: EntityRecord =>
          entityRec.name -> Success(attributeVal)
        }).toMap
        case None => Map("" -> Success(attributeVal))
      }
      case Failure(e) => throw new ParseCancellationException(s"Something went wrong! $e")
    }
  }


  override def visitLookup(ctx: LookupContext): Map[String, Try[Iterable[AttributeValue]]] = {
    val expression = ctx.getText

    val abc: ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]] = slickEvaluator.evalFinalAttribute(workspaceContext, expression)

    Await.result(dataSource.inTransaction(_ => abc), 1.minute)
  }

  override def aggregateResult(aggregate: Map[String, Try[Iterable[AttributeValue]]],
                               nextResult: Map[String, Try[Iterable[AttributeValue]]]): Map[String, Try[Iterable[AttributeValue]]] = {

    def combineMap[A, B](maps: List[Map[A, B]])(func: (B, B) => B): Map[A, B] = {
      ???
    }

    aggregate.map {
      case (key, aggregateAttrListTry) => {
        nextResult.get(key) match {
          case Some(nextResultAttrListTry) => (aggregateAttrListTry, nextResultAttrListTry) match {
            case (Success(aggregateAttrList), Success(nextResultAttrList)) => (key, Success(aggregateAttrList ++ nextResultAttrList))
            case (Success(_), Failure(_)) || (Failure(e), Success(_)) => (key, Failure(e))
            case (Failure(e1), Failure(_)) => (key, Failure(e1))
          }
          case None => (key, aggregateAttrListTry)
        }
      }
    }
  }
}
