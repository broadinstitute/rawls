package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.TerraExpressionParser.EntityLookupContext
import org.broadinstitute.dsde.rawls.expressions.{BoundOutputExpression, OutputExpression, ThisEntityTarget}
import org.broadinstitute.dsde.rawls.model.{Attribute, ErrorReport}

import scala.util.{Failure, Try}

/** Output expressions don't allow for entity references (relations) in the middle or for any JSON. Entity
  * expressions are only allowed if there is a root entity type defined */
class LocalOutputExpressionValidationVisitor(rootEntityTypeOption: Option[String])
    extends OutputExpressionValidationVisitor {

  override def visitEntityLookup(ctx: EntityLookupContext): Try[Attribute => OutputExpression] =
    rootEntityTypeOption match {
      case None =>
        Failure(
          new RawlsExceptionWithErrorReport(
            ErrorReport(
              StatusCodes.BadRequest,
              "Output expressions beginning with \"this.\" are only allowed when running with workspace data model. However, workspace attributes can be used."
            )
          )
        )
      case Some(rootEntityType) =>
        val attributeName = AntlrTerraExpressionParser.toAttributeName(ctx.attributeName())
        for {
          _ <- Try(validateAttributeName(attributeName, rootEntityType))
          _ <- visitChildren(ctx)
        } yield (attribute: Attribute) => BoundOutputExpression(ThisEntityTarget, attributeName, attribute)
    }
}
