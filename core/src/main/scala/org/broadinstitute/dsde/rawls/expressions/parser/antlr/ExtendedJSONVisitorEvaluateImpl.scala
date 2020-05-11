package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import org.antlr.v4.runtime.misc.ParseCancellationException
import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import org.broadinstitute.dsde.rawls.dataaccess.slick.{EntityRecord, ReadWriteAction}
import org.broadinstitute.dsde.rawls.expressions.SlickExpressionEvaluator
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.ExtendedJSONParser.LookupContext
import org.broadinstitute.dsde.rawls.model.{AttributeBoolean, AttributeNull, AttributeNumber, AttributeString, AttributeValue}
import spray.json._

import scala.util.{Failure, Success, Try}

class ExtendedJSONVisitorEvaluateImpl(expression: String,
                                      slickEvaluator: SlickExpressionEvaluator,
                                      workspaceContext: SlickWorkspaceContext) extends
  ExtendedJSONBaseVisitor[Seq[ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]]]] {
  import slickEvaluator.parser.driver.api._

  override def visitLookup(ctx: LookupContext): Seq[ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]]] = {
    val expression = ctx.getText

    Seq(slickEvaluator.evalFinalAttribute(workspaceContext, expression))
  }

  override def visitLiteral(ctx: ExtendedJSONParser.LiteralContext): Seq[ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]]] = {
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
      case Success(attributeVal) => Seq(DBIO.successful(slickEvaluator.rootEntities match {
        case Some(entities) => (entities map { entityRec: EntityRecord =>
          entityRec.name -> Success(attributeVal)
        }).toMap
        case None => Map("" -> Success(attributeVal))
      }))
      case Failure(e) => throw new ParseCancellationException(s"Something went wrong! $e")
    }
  }

  override def defaultResult(): Seq[ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]]] = {
    Seq(DBIO.successful(Map("" -> Success(Seq(AttributeNull)))))
  }
}
