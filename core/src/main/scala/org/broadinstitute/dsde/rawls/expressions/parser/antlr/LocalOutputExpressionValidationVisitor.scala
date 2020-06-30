package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.TerraExpressionParser.{ArrContext, LiteralContext, ObjContext, RelationContext}
import org.broadinstitute.dsde.rawls.expressions.{BoundOutputExpression, OutputExpression, ThisEntityTarget, UnboundOutputExpression, WorkspaceTarget}
import org.broadinstitute.dsde.rawls.model.{Attributable, Attribute, AttributeNull, ErrorReport, ErrorReportSource}
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport, StringValidationUtils}

import scala.util.{Failure, Success, Try}

/** Output expressions don't allow for entity references (relations) in the middle or for any JSON. They must
  * be of the form this.attribute or workspace.attribute */
class LocalOutputExpressionValidationVisitor(rootEntityTypeOption: Option[String])
  extends TerraExpressionBaseVisitor[Try[Attribute => OutputExpression]]
  with StringValidationUtils {

  override implicit val errorReportSource: ErrorReportSource = ErrorReportSource("rawls")


  override def defaultResult(): Try[Attribute => OutputExpression] = Success(_ => UnboundOutputExpression)

  override def aggregateResult(aggregateT: Try[Attribute => OutputExpression], nextResultT: Try[Attribute => OutputExpression]): Try[Attribute => OutputExpression] = {
    for {
      aggregate <- aggregateT
      nextResult <- nextResultT
    } yield {
      // return whichever of aggregate and nextResult is bound
      if (aggregate(AttributeNull) == UnboundOutputExpression) nextResult
      else aggregate
    }
  }

  override def visitRelation(ctx: RelationContext): Try[Attribute => OutputExpression] = {
    Failure(new RawlsException("Entity references not permitted in the middle of output expressions"))
  }

  override def visitEntityLookup(ctx: TerraExpressionParser.EntityLookupContext): Try[Attribute => OutputExpression] = {
    rootEntityTypeOption match {
      case None => Failure(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Output expressions beginning with \"this.\" are only allowed when running with workspace data model. However, workspace attributes can be used.")))
      case Some(rootEntityType) =>
        val attributeName = AntlrTerraExpressionParser.toAttributeName(ctx.attributeName())
        for {
        _ <- Try(validateAttributeName(attributeName, rootEntityType))
        _ <- visitChildren(ctx)
      } yield (attribute: Attribute) => BoundOutputExpression(ThisEntityTarget, attributeName, attribute)
    }
  }

  override def visitWorkspaceAttributeLookup(ctx: TerraExpressionParser.WorkspaceAttributeLookupContext): Try[Attribute => OutputExpression] = {
    val attributeName = AntlrTerraExpressionParser.toAttributeName(ctx.attributeName())
    Try(validateAttributeName(attributeName, Attributable.workspaceEntityType)).map { _ =>
      (attribute: Attribute) => BoundOutputExpression(WorkspaceTarget, attributeName, attribute)
    }
  }

  override def visitObj(ctx: ObjContext): Try[Attribute => OutputExpression] = {
    Failure(new RawlsException("Output expressions cannot be JSON"))
  }

  override def visitArr(ctx: ArrContext): Try[Attribute => OutputExpression] = {
    Failure(new RawlsException("Output expressions cannot be JSON"))
  }

  override def visitLiteral(ctx: LiteralContext): Try[Attribute => OutputExpression] = {
    Failure(new RawlsException("Output expressions cannot be JSON"))
  }
}
