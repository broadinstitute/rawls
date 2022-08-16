package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.TerraExpressionParser.EntityLookupContext

import scala.util.{Failure, Success, Try}

class LocalInputExpressionValidationVisitor(allowRootEntity: Boolean) extends TerraExpressionBaseVisitor[Try[Unit]] {

  override def defaultResult() = Success(())

  override def aggregateResult(aggregate: Try[Unit], nextResult: Try[Unit]): Try[Unit] =
    aggregate.flatMap(_ => nextResult)

  // Entity lookup nodes are only allowed if allowRootEntity is true
  override def visitEntityLookup(ctx: EntityLookupContext): Try[Unit] =
    if (allowRootEntity) {
      // We don't want to short circuit here as child classes may want to validate all child nodes are valid
      visitChildren(ctx.getRuleContext)
    } else {
      Failure(
        new RawlsException(
          "Input expressions beginning with \"this.\" are only allowed when running with workspace data model. However, workspace attributes can be used."
        )
      )
    }
}
