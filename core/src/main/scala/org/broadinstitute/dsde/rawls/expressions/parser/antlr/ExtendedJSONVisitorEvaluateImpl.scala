package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import org.broadinstitute.dsde.rawls.dataaccess.slick.ReadWriteAction
import org.broadinstitute.dsde.rawls.expressions.SlickExpressionEvaluator
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.ExtendedJSONParser.LookupContext
import org.broadinstitute.dsde.rawls.model.AttributeValue

import scala.util.Try

class ExtendedJSONVisitorEvaluateImpl(expression: String,
                                      slickEvaluator: SlickExpressionEvaluator,
                                      workspaceContext: SlickWorkspaceContext) extends
  ExtendedJSONBaseVisitor[ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]]] {
//  import slickEvaluator.parser.driver.api._

  /**
    * {@inheritDoc }
    *
    * <p>The default implementation returns the result of calling
    * {@link #visitChildren} on {@code ctx}.</p>
    */
  override def visitLookup(ctx: LookupContext): ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]] = {
    val expression = ctx.getText

    slickEvaluator.evalFinalAttribute(workspaceContext, expression)
  }

//  override def aggregateResult(aggregate: ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]],
//                               nextResult: ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]]):
//  ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]] = {
//    ???
//  }
//
//  override def defaultResult(): ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]] = {
//    DBIO.successful("" -> Success(AttributeNull))
//  }
}
