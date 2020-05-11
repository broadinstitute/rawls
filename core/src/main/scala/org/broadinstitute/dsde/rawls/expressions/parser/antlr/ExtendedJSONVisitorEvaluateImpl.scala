package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import org.antlr.v4.runtime.misc.ParseCancellationException
import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import org.broadinstitute.dsde.rawls.dataaccess.slick.{EntityRecord, ReadWriteAction}
import org.broadinstitute.dsde.rawls.expressions.SlickExpressionEvaluator
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.ExtendedJSONParser.LookupContext
import org.broadinstitute.dsde.rawls.model.{AttributeBoolean, AttributeNull, AttributeNumber, AttributeString, AttributeValue}
import spray.json._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

class ExtendedJSONVisitorEvaluateImpl(expression: String,
                                      slickEvaluator: SlickExpressionEvaluator,
                                      workspaceContext: SlickWorkspaceContext)
                                     (implicit executionContext: ExecutionContext) extends
  ExtendedJSONBaseVisitor[ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]]] {
  import slickEvaluator.parser.driver.api._

  /*
  {"example": {"abc": this.ref, "def": this.another}}
   */

  override def visitLookup(ctx: LookupContext): ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]] = {
    val expression = ctx.getText

    slickEvaluator.evalFinalAttribute(workspaceContext, expression)

    // [10019 -> Seq("gs://abc/")]
    // [10019 -> Seq("gs://def/")]
  }

  override def visitLiteral(ctx: ExtendedJSONParser.LiteralContext): ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]] = {
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
      case Success(attributeVal) => DBIO.successful(slickEvaluator.rootEntities match {
        case Some(entities) => (entities map { entityRec: EntityRecord =>
          entityRec.name -> Success(attributeVal)
        }).toMap
        case None => Map("" -> Success(attributeVal))
      })
      case Failure(e) => throw new ParseCancellationException(s"Something went wrong! $e")
    }
  }

  /*
    [1, 2, "abc"]
    10019 -> [AttributeNumber(1), ]
   */


  override def aggregateResult(aggregate: ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]],
                               nextResult: ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]]): ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]] = {

    // TODO: replace with Cats Semi-group for maps or implement something smarter. `toMap` will replace the value if there 2 tuples with same keys
    //  where only the latter will be saved
    DBIO.sequence(Seq(aggregate, nextResult)).map { seqOfMaps =>
      seqOfMaps.flatten.toMap
    }
  }

  override def defaultResult(): ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]] = {
    DBIO.successful(Map("" -> Success(Seq(AttributeNull))))
  }
}
