package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import org.broadinstitute.dsde.rawls.expressions.parser.antlr.TerraExpressionParser._
import org.broadinstitute.dsde.rawls.expressions.{
  BoundOutputExpression,
  OutputExpression,
  UnboundOutputExpression,
  WorkspaceTarget
}
import org.broadinstitute.dsde.rawls.model.{Attributable, Attribute, AttributeNull, ErrorReportSource}
import org.broadinstitute.dsde.rawls.{RawlsException, StringValidationUtils}

import scala.util.{Failure, Success, Try}

/** Output expressions don't allow for entity references (relations) in the middle or for any JSON. They must
  * be of the form workspace.attribute */
class OutputExpressionValidationVisitor
    extends TerraExpressionBaseVisitor[Try[Attribute => OutputExpression]]
    with StringValidationUtils {

  implicit override val errorReportSource: ErrorReportSource = ErrorReportSource("rawls")

  override def defaultResult(): Try[Attribute => OutputExpression] = Success(_ => UnboundOutputExpression)

  override def aggregateResult(aggregateT: Try[Attribute => OutputExpression],
                               nextResultT: Try[Attribute => OutputExpression]
  ): Try[Attribute => OutputExpression] =
    for {
      aggregate <- aggregateT
      nextResult <- nextResultT
    } yield
    // return whichever of aggregate and nextResult is bound
    if (aggregate(AttributeNull) == UnboundOutputExpression) nextResult
    else aggregate

  override def visitRelation(ctx: RelationContext): Try[Attribute => OutputExpression] =
    Failure(new RawlsException("Entity references not permitted in the middle of output expressions"))

  override def visitWorkspaceAttributeLookup(
    ctx: WorkspaceAttributeLookupContext
  ): Try[Attribute => OutputExpression] = {
    val attributeName = AntlrTerraExpressionParser.toAttributeName(ctx.attributeName())
    Try(validateAttributeName(attributeName, Attributable.workspaceEntityType)).map { _ => (attribute: Attribute) =>
      BoundOutputExpression(WorkspaceTarget, attributeName, attribute)
    }
  }

  override def visitObj(ctx: ObjContext): Try[Attribute => OutputExpression] =
    Failure(new RawlsException("Output expressions cannot be JSON"))

  override def visitArr(ctx: ArrContext): Try[Attribute => OutputExpression] =
    Failure(new RawlsException("Output expressions cannot be JSON"))

  override def visitLiteral(ctx: LiteralContext): Try[Attribute => OutputExpression] =
    Failure(new RawlsException("Output expressions cannot be JSON"))
}
