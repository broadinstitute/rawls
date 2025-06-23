package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import org.broadinstitute.dsde.rawls.expressions.parser.antlr.CompactEvaluateVisitor.ExpressionLookup
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.TerraExpressionParser.{
  EntityLookupContext,
  RelationContext
}
import org.broadinstitute.dsde.rawls.model.AttributeName.toDelimitedName
import org.broadinstitute.dsde.rawls.model.AttributeValue

import scala.jdk.CollectionConverters._
import scala.util.{Success, Try}

object CompactEvaluateVisitor {
  case class ExpressionLookup(
    expression: String, // the original expression or literal
    relations: List[RelationContext],
    attributeName: Option[String], // None for literals
    values: Seq[AttributeValue] // the result of evaluating this lookup (from DB or literal) //TODO is this ever used
  )
}

class CompactEvaluateVisitor extends TerraExpressionBaseVisitor[Seq[ExpressionLookup]] {

  override protected def aggregateResult(aggregate: Seq[ExpressionLookup],
                                         nextResult: Seq[ExpressionLookup]
  ): Seq[ExpressionLookup] = aggregate ++ nextResult

  override protected def defaultResult(): Seq[ExpressionLookup] = Seq.empty[ExpressionLookup]

  override def visitEntityLookup(ctx: EntityLookupContext): Seq[ExpressionLookup] =
    Seq(
      ExpressionLookup(
        expression = ctx.getText,
        relations = ctx.relation().asScala.toList,
        attributeName = Some(toDelimitedName(AntlrTerraExpressionParser.toAttributeName(ctx.attributeName()))),
        values = Seq.empty
      )
    )

  override def visitWorkspaceAttributeLookup(
    ctx: TerraExpressionParser.WorkspaceAttributeLookupContext
  ): Seq[ExpressionLookup] =
    Seq(
      ExpressionLookup(
        expression = ctx.getText,
        relations = List.empty,
        attributeName = Some(ctx.attributeName().name().getText),
        values = Seq.empty
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
        attributeName = Some(ctx.attributeName().name().getText),
        values = Seq.empty
      )
    )

}
