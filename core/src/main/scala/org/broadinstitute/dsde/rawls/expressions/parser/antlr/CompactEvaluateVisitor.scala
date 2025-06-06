package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import org.broadinstitute.dsde.rawls.expressions.parser.antlr.CompactEvaluateVisitor.ExpressionLookup
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.TerraExpressionParser.{
  EntityLookupContext,
  RelationContext
}
import org.broadinstitute.dsde.rawls.model.AttributeValue

import scala.jdk.CollectionConverters._
import scala.util.{Success, Try}

object CompactEvaluateVisitor {
  case class ExpressionLookup(
    expression: String, // the original expression or literal
    relations: List[RelationContext],
    attributeName: Option[String], // None for literals
    values: Seq[AttributeValue] // the result of evaluating this lookup (from DB or literal)
  ) {
    // TODO is this needed anywhere
    def toExpressionAndResult(
      lookup: ExpressionLookup,
      entityNames: Seq[String]
    ): (String, Map[String, Try[Iterable[AttributeValue]]]) = {
      val resultMap =
        if (lookup.attributeName.isDefined)
          // For entity lookups, build the map from DB results (already in lookup.values)
          entityNames.map(name => name -> Success(lookup.values)).toMap
        else
          // For literals, just map the root entity to the literal value
          entityNames.map(name => name -> Success(lookup.values)).toMap

      (lookup.expression, resultMap)
    }
  }

}

//TODO will this need to be a seq or can we always get by with just one?
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
        attributeName = Some(ctx.attributeName().name().getText),
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
