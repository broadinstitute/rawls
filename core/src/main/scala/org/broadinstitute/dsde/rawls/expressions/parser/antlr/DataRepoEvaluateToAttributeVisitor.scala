package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationSupport.LookupExpression
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.TerraExpressionParser.EntityLookupContext

import scala.collection.JavaConverters._

case class ParsedEntityLookupExpression(relationships: List[String], columnName: String, expression: LookupExpression)

class DataRepoEvaluateToAttributeVisitor() extends TerraExpressionBaseVisitor[Seq[ParsedEntityLookupExpression]] {
  override def defaultResult(): Seq[ParsedEntityLookupExpression] = Seq.empty

  override def aggregateResult(aggregate: Seq[ParsedEntityLookupExpression], nextResult: Seq[ParsedEntityLookupExpression]): Seq[ParsedEntityLookupExpression] = {
    aggregate ++ nextResult
  }

  override def visitEntityLookup(ctx: EntityLookupContext): Seq[ParsedEntityLookupExpression] = {
    val relations = ctx.relation().asScala.toList
    Seq(ParsedEntityLookupExpression(relations.map(_.attributeName().getText), ctx.attributeName().getText.toLowerCase, ctx.getText))
  }
}

