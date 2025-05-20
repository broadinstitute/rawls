package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import org.broadinstitute.dsde.rawls.expressions.parser.antlr.CompactEvaluateVisitor.AttributeLookup
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.TerraExpressionParser.{
  EntityLookupContext,
  RelationContext
}

import scala.jdk.CollectionConverters._

object CompactEvaluateVisitor {
  // TODO should attributeName be an AttributeName
  case class AttributeLookup(relations: List[RelationContext], attributeName: String)

}

//TODO will this need to be a seq or can we always get by with just one?
class CompactEvaluateVisitor extends TerraExpressionBaseVisitor[Seq[AttributeLookup]] {

  override protected def aggregateResult(aggregate: Seq[AttributeLookup],
                                         nextResult: Seq[AttributeLookup]
  ): Seq[AttributeLookup] =
    Seq(aggregate, nextResult).flatten

  override protected def defaultResult(): Seq[AttributeLookup] = Seq.empty[AttributeLookup]

  override def visitEntityLookup(ctx: EntityLookupContext): Seq[AttributeLookup] =
    Seq(
      AttributeLookup(ctx.relation().asScala.toList, ctx.attributeName().name().getText)
    )

  override def visitWorkspaceAttributeLookup(
    ctx: TerraExpressionParser.WorkspaceAttributeLookupContext
  ): Seq[AttributeLookup] = Seq(
    AttributeLookup(List.empty, ctx.attributeName().name().getText)
  )

  // TODO this is wrong
  // Essentially this should be visitEntityLookup on ctx.children(1) but I'm not sure a reliable way to do that
  override def visitWorkspaceEntityLookup(
    ctx: TerraExpressionParser.WorkspaceEntityLookupContext
  ): Seq[AttributeLookup] = Seq(
    AttributeLookup(ctx.relation().asScala.toList, ctx.attributeName().name().getText)
  )

}
