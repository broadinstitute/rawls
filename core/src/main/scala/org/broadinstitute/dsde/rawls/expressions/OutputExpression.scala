package org.broadinstitute.dsde.rawls.expressions

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.{AntlrTerraExpressionParser, LocalOutputExpressionValidationVisitor}
import org.broadinstitute.dsde.rawls.model.{Attribute, AttributeName, AttributeNull, ErrorReport}

import scala.util.{Failure, Success, Try}

sealed trait OutputExpressionTarget
case object ThisEntityTarget extends OutputExpressionTarget
case object WorkspaceTarget extends OutputExpressionTarget

sealed trait OutputExpression
case object UnboundOutputExpression extends OutputExpression
case class BoundOutputExpression(target: OutputExpressionTarget, attributeName: AttributeName, attribute: Attribute) extends OutputExpression

object OutputExpression {
  def validate(expr: String, rootEntityTypeOption: Option[String]): Try[Unit] = {
    // build also validates so just use that but ignore the value
    build(expr, AttributeNull, rootEntityTypeOption).map(_ => ())
  }

  def build(expr: String, attribute: Attribute, rootEntityTypeOption: Option[String]): Try[OutputExpression] = {
    if (expr.isEmpty) Success(UnboundOutputExpression)
    else {
      val extendedJsonParser = AntlrTerraExpressionParser.getParser(expr)
      val visitor = new LocalOutputExpressionValidationVisitor(rootEntityTypeOption)

      for {
        parseTree <- Try(extendedJsonParser.root()).recoverWith {
          case regrets: RawlsException =>
            Failure(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"Invalid output expression: $expr", regrets)))
        }
        boundExprFunc <- visitor.visit(parseTree)
      } yield boundExprFunc(attribute)
    }
  }
}
