package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import org.broadinstitute.dsde.rawls.dataaccess.slick.ReadWriteAction
import org.broadinstitute.dsde.rawls.expressions.SlickExpressionEvaluator
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.ExtendedJSONParser.{EntityLookupContext, WorkspaceAttributeLookupContext, WorkspaceEntityLookupContext}
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.LocalEvaluateToAttributeVisitor.ExpressionAndResult
import org.broadinstitute.dsde.rawls.model.{AttributeValue, Workspace}

import scala.concurrent.ExecutionContext
import scala.util.Try

class LocalEvaluateToAttributeVisitor(workspace: Workspace, slickEvaluator: SlickExpressionEvaluator)
                                     (implicit executionContext: ExecutionContext)
  extends ExtendedJSONBaseVisitor[ReadWriteAction[Seq[ExpressionAndResult]]] {

  override def defaultResult(): ReadWriteAction[Seq[ExpressionAndResult]] = {
    import slickEvaluator.dataAccess.driver.api._

    DBIO.successful(Seq.empty[ExpressionAndResult])
  }

  override def aggregateResult(aggregate: ReadWriteAction[Seq[ExpressionAndResult]],
                               nextResult: ReadWriteAction[Seq[ExpressionAndResult]]
                              ): ReadWriteAction[Seq[ExpressionAndResult]] = {
    import slickEvaluator.dataAccess.driver.api._

    DBIO.sequence(Seq(aggregate, nextResult)).map(_.flatten)
  }

  override def visitEntityLookup(ctx: EntityLookupContext): ReadWriteAction[Seq[ExpressionAndResult]] = {
    slickEvaluator.evalEntityLookupFinalAttribute(workspace, ctx).map { result =>
      Seq((ctx.getText, result))
    }
  }

  override def visitWorkspaceEntityLookup(ctx: WorkspaceEntityLookupContext): ReadWriteAction[Seq[ExpressionAndResult]] = {
    slickEvaluator.evalWorkspaceEntityLookupFinalAttribute(workspace, ctx).map { result =>
      Seq((ctx.getText, result))
    }
  }

  override def visitWorkspaceAttributeLookup(ctx: WorkspaceAttributeLookupContext): ReadWriteAction[Seq[ExpressionAndResult]] = {
    slickEvaluator.evalWorkspaceAttributeLookupFinalAttribute(workspace, ctx).map { result =>
      Seq((ctx.getText, result))
    }
  }
}

object LocalEvaluateToAttributeVisitor {
  type EntityName = String
  type LookupExpression = String

  type ExpressionAndResult = (LookupExpression, Map[EntityName, Try[Iterable[AttributeValue]]])
}
