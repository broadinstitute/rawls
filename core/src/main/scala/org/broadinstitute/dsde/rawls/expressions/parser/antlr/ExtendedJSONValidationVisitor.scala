package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import cats.instances.try_._
import cats.syntax.functor._
import org.broadinstitute.dsde.rawls.expressions.SlickExpressionParser

import scala.util.{Failure, Success, Try}

class ExtendedJSONValidationVisitor(allowRootEntity: Boolean, parser: SlickExpressionParser) extends ExtendedJSONBaseVisitor[Try[Unit]] {

  override def defaultResult() = Success(())

  override def aggregateResult(aggregate: Try[Unit], nextResult: Try[Unit]): Try[Unit] = {
    aggregate match {
      case Failure(_) => aggregate
      case Success(_) => nextResult
    }
  }

  /**
    * For a lookup node, use the SlickExpressionParser to validate the expression
    */
  override def visitLookup(ctx: ExtendedJSONParser.LookupContext): Try[Unit] = {
    val expression = ctx.getText
    parser.parseAttributeExpr(expression, allowRootEntity).void
  }
}
