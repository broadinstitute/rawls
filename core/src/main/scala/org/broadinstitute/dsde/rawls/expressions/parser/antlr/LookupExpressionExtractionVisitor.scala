package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationSupport.LookupExpression
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.TerraExpressionParser.LookupContext

class LookupExpressionExtractionVisitor extends TerraExpressionBaseVisitor[Seq[LookupExpression]] {
  override def defaultResult(): Seq[LookupExpression] = Seq.empty

  override def aggregateResult(aggregate: Seq[LookupExpression],
                               nextResult: Seq[LookupExpression]
  ): Seq[LookupExpression] =
    aggregate ++ nextResult

  override def visitLookup(ctx: LookupContext): scala.Seq[LookupExpression] = Seq(ctx.getText)
}
