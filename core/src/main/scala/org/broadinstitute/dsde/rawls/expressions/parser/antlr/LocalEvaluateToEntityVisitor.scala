package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import org.broadinstitute.dsde.rawls.dataaccess.slick.{EntityRecord, ReadWriteAction}
import org.broadinstitute.dsde.rawls.expressions.SlickExpressionEvaluator
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.TerraExpressionParser.{
  EntityLookupContext,
  WorkspaceAttributeLookupContext,
  WorkspaceEntityLookupContext
}
import org.broadinstitute.dsde.rawls.model.Workspace

import scala.concurrent.ExecutionContext

class LocalEvaluateToEntityVisitor(workspace: Workspace, slickEvaluator: SlickExpressionEvaluator)(implicit
  executionContext: ExecutionContext
) extends TerraExpressionBaseVisitor[ReadWriteAction[Iterable[EntityRecord]]] {

  override def defaultResult(): ReadWriteAction[Iterable[EntityRecord]] = {
    import slickEvaluator.dataAccess.driver.api._

    DBIO.successful(Seq.empty[EntityRecord])
  }

  override def aggregateResult(aggregate: ReadWriteAction[Iterable[EntityRecord]],
                               nextResult: ReadWriteAction[Iterable[EntityRecord]]
  ): ReadWriteAction[Iterable[EntityRecord]] = {
    import slickEvaluator.dataAccess.driver.api._

    DBIO.sequence(Seq(aggregate, nextResult)).map(_.flatten)
  }

  override def visitWorkspaceEntityLookup(ctx: WorkspaceEntityLookupContext): ReadWriteAction[Iterable[EntityRecord]] =
    slickEvaluator.evalWorkspaceEntityLookupFinalEntity(workspace, ctx)

  override def visitEntityLookup(ctx: EntityLookupContext): ReadWriteAction[Iterable[EntityRecord]] =
    slickEvaluator.evalEntityLookupFinalEntity(workspace, ctx)

  override def visitWorkspaceAttributeLookup(
    ctx: WorkspaceAttributeLookupContext
  ): ReadWriteAction[Iterable[EntityRecord]] =
    slickEvaluator.evalWorkspaceAttributeLookupFinalEntity(workspace, ctx)
}
