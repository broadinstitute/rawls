package org.broadinstitute.dsde.rawls.expressions

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.model.{Attributable, Attribute, AttributeName, ErrorReport, ErrorReportSource}
import org.broadinstitute.dsde.rawls.{RawlsFatalExceptionWithErrorReport, StringValidationUtils}

import scala.util.{Failure, Success, Try}

sealed trait OutputExpressionTarget { val root: String }
case object ThisEntityTarget extends OutputExpressionTarget { override val root = "this." }
case object WorkspaceTarget extends OutputExpressionTarget { override val root = "workspace." }

sealed trait OutputExpression
case object UnboundOutputExpression extends OutputExpression
case class BoundOutputExpression(target: OutputExpressionTarget, attributeName: AttributeName, attribute: Attribute) extends OutputExpression

object BoundOutputExpression extends StringValidationUtils {
  override implicit val errorReportSource: ErrorReportSource = ErrorReportSource("rawls")

  def tryAttributeName(target: OutputExpressionTarget, expr: String): Try[AttributeName] = {
    Try {
      val attributeName = AttributeName.fromDelimitedName(expr.stripPrefix(target.root))
      validateUserDefinedString(attributeName.name)
      attributeName
    }
  }

  def tryValidate(rootEntityTypeOption: Option[String])
                 (target: OutputExpressionTarget,
                  expr: String): Try[Unit] = {
    tryAttributeName(target, expr) map { attributeName =>
      target match {
        case ThisEntityTarget => rootEntityTypeOption.foreach(validateAttributeName(attributeName, _))
        case WorkspaceTarget => validateAttributeName(attributeName, Attributable.workspaceEntityType)
      }
    }
  }

  def tryBuild(attribute: Attribute)(target: OutputExpressionTarget, expr: String): Try[BoundOutputExpression] = {
    tryAttributeName(target, expr).map(BoundOutputExpression(target, _, attribute))
  }

  def tryParse[A](expr: String, parser: (OutputExpressionTarget, String) => Try[A]): Try[A] = {
    if (expr.startsWith(ThisEntityTarget.root)) {
      parser(ThisEntityTarget, expr)
    } else if (expr.startsWith(WorkspaceTarget.root)) {
      parser(WorkspaceTarget, expr)
    } else {
      Failure(new RawlsFatalExceptionWithErrorReport(errorReport = ErrorReport(
        message = s"Invalid output expression: $expr",
        statusCode = StatusCodes.BadRequest
      )))
    }
  }
}

object OutputExpression {
  def validate(expr: String, rootEntityTypeOption: Option[String]): Try[Unit] = {
    if (expr.isEmpty) Success(())
    else BoundOutputExpression.tryParse(expr, BoundOutputExpression.tryValidate(rootEntityTypeOption))
  }

  def build(expr: String, attribute: Attribute): Try[OutputExpression] = {
    if (expr.isEmpty) Success(UnboundOutputExpression)
    else BoundOutputExpression.tryParse(expr, BoundOutputExpression.tryBuild(attribute))
  }
}
