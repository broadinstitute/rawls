package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import cats.instances.try_._
import cats.syntax.functor._
import org.broadinstitute.dsde.rawls.expressions.SlickExpressionParser

import scala.util.{Failure, Success, Try}

class ExtendedJSONVisitorImpl(allowRootEntity: Boolean, parser: SlickExpressionParser) extends ExtendedJSONBaseVisitor[Try[Unit]] {

  /**
    * {@inheritDoc }
    *
    * <p>The default implementation returns the result of calling
    * {@link #visitChildren} on {@code ctx}.</p>
    */
  override def visitLookup(ctx: ExtendedJSONParser.LookupContext): Try[Unit] = {
    val expression = ctx.getText
    parser.parseAttributeExpr(expression, allowRootEntity).void
  }

  override def aggregateResult(aggregate: Try[Unit], nextResult: Try[Unit]): Try[Unit] = {
    aggregate match {
      case Failure(_) => aggregate
      case Success(_) => nextResult
    }
  }

  override def defaultResult(): Try[Unit] = Success(())
}
