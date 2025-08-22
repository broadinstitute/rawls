package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import org.broadinstitute.dsde.rawls.expressions.parser.antlr.CompactEvaluateVisitor.ExpressionLookup
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.TerraExpressionParser.{
  EntityLookupContext,
  RelationContext
}
import org.broadinstitute.dsde.rawls.model.AttributeName.toDelimitedName

import scala.jdk.CollectionConverters._

object CompactEvaluateVisitor {
  case class ExpressionLookup(
    expression: String, // the original expression or literal
    relations: List[RelationContext],
    attributeName: Option[String] // None for literals
  )
}

class CompactEvaluateVisitor extends TerraExpressionBaseVisitor[Seq[ExpressionLookup]] {

  override protected def aggregateResult(aggregate: Seq[ExpressionLookup],
                                         nextResult: Seq[ExpressionLookup]
  ): Seq[ExpressionLookup] = aggregate ++ nextResult

  override protected def defaultResult(): Seq[ExpressionLookup] = Seq.empty[ExpressionLookup]

  override def visitEntityLookup(ctx: EntityLookupContext): Seq[ExpressionLookup] = {
    // handle nulls in ctx.attributeName()
    val attributeNameOption = Option(ctx.attributeName()).map { attrName =>
      toDelimitedName(AntlrTerraExpressionParser.toAttributeName(attrName))
    }
    Seq(
      ExpressionLookup(
        expression = ctx.getText,
        relations = ctx.relation().asScala.toList,
        attributeName = attributeNameOption
      )
    )
  }

  override def visitWorkspaceAttributeLookup(
    ctx: TerraExpressionParser.WorkspaceAttributeLookupContext
  ): Seq[ExpressionLookup] =
    Seq(
      ExpressionLookup(
        expression = ctx.getText,
        relations = List.empty,
        attributeName = Some(ctx.attributeName().name().getText)
      )
    )

  // TODO this is wrong
  // Essentially this should be visitEntityLookup on ctx.children(1) but I'm not sure a reliable way to do that
  override def visitWorkspaceEntityLookup(
    ctx: TerraExpressionParser.WorkspaceEntityLookupContext
  ): Seq[ExpressionLookup] =
    Seq(
      ExpressionLookup(
        expression = ctx.getText,
        relations = ctx.relation().asScala.toList,
        attributeName = Some(ctx.attributeName().name().getText)
      )
    )

}
