package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationSupport.LookupExpression
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.TerraExpressionParser.EntityLookupContext

import scala.jdk.CollectionConverters._

/**
  * Encapsulates the information that we care about a lookup expression for the purpose of constructing a query
  * @param relationshipPath the chain of relationship names in the expression: if the expression looks like
  *                         this.(relationship_name.)*columnName, the relationshipPath is each relationship_name in order
  * @param columnName last part of the expression
  * @param expression the expression itself
  */
case class ParsedEntityLookupExpression(relationshipPath: Seq[String], columnName: String, expression: LookupExpression)

class DataRepoEvaluateToAttributeVisitor() extends TerraExpressionBaseVisitor[Seq[ParsedEntityLookupExpression]] {
  override def defaultResult(): Seq[ParsedEntityLookupExpression] = Seq.empty

  override def aggregateResult(aggregate: Seq[ParsedEntityLookupExpression],
                               nextResult: Seq[ParsedEntityLookupExpression]
  ): Seq[ParsedEntityLookupExpression] =
    aggregate ++ nextResult

  override def visitEntityLookup(ctx: EntityLookupContext): Seq[ParsedEntityLookupExpression] = {
    val relations = ctx.relation().asScala
    Seq(
      ParsedEntityLookupExpression(relations.map(_.attributeName().getText).toList,
                                   ctx.attributeName().getText.toLowerCase,
                                   ctx.getText
      )
    )
  }
}
