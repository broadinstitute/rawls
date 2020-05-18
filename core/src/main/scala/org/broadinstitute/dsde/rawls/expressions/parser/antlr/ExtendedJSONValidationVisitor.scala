package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import cats.instances.try_._
import cats.syntax.functor._
import org.broadinstitute.dsde.rawls.expressions.SlickExpressionParser

import scala.util.{Failure, Success, Try}

class ExtendedJSONValidationVisitor(allowRootEntity: Boolean, parser: SlickExpressionParser) extends ExtendedJSONBaseVisitor[Try[Unit]] {

  /**
    * Gets the default value returned by visitor methods. The base implementation returns null.
    *
    * @return The default value returned by visitor methods.
    */
  override def defaultResult() = Success(())

  /**
    * Aggregates the results of visiting multiple children of a node.
    *
    * @param aggregate The previous aggregate value. In the default implementation, the aggregate value is initialized to
    *                  {defaultResult()}, which is passed as the {aggregate} argument to this method after the first
    *                  child node is visited.
    * @param nextResult The result of the immediately preceeding call to visit a child node.
    * @return The updated aggregate result.
    */
  override def aggregateResult(aggregate: Try[Unit], nextResult: Try[Unit]): Try[Unit] = {
    aggregate match {
      case Failure(_) => aggregate
      case Success(_) => nextResult
    }
  }

  /**
    * Visit a parse tree produced by ExtendedJSONParser#lookup
    *
    * @param ctx the parse tree
    * @return the visitor result
    */
  override def visitLookup(ctx: ExtendedJSONParser.LookupContext): Try[Unit] = {
    val expression = ctx.getText
    parser.parseAttributeExpr(expression, allowRootEntity).void
  }
}
