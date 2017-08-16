package org.broadinstitute.dsde.rawls.expressions

import org.broadinstitute.dsde.rawls.{RawlsException, StringValidationUtils}
import org.broadinstitute.dsde.rawls.model.{Attribute, AttributeName, ErrorReportSource}

import scala.util.{Failure, Try}

sealed trait OutputExpressionTarget { val root: String }
case object ThisEntityTarget extends OutputExpressionTarget { override val root = "this." }
case object WorkspaceTarget extends OutputExpressionTarget { override val root = "workspace." }

sealed trait OutputExpression
case object UnboundOutputExpression extends OutputExpression
case class BoundOutputExpression(target: OutputExpressionTarget, attributeName: AttributeName, attribute: Attribute) extends OutputExpression

object BoundOutputExpression extends StringValidationUtils {
  override implicit val errorReportSource = ErrorReportSource("rawls")

  def tryParse(target: OutputExpressionTarget, expr: String, attribute: Attribute): Try[BoundOutputExpression] = {
    if (expr.startsWith(target.root)) Try {
      val attributeName = AttributeName.fromDelimitedName(expr.stripPrefix(target.root))
      validateUserDefinedString(attributeName.name)

      BoundOutputExpression(target, attributeName, attribute)
    }
    else Failure(new RawlsException(s"Invalid output expression: $expr"))
  }

  def apply(expr: String, attribute: Attribute): BoundOutputExpression = {
    val parseResult = tryParse(ThisEntityTarget, expr, attribute) recoverWith { case _ => tryParse(WorkspaceTarget, expr, attribute) }
    parseResult.get
  }
}

object OutputExpression {
  def apply(expr: String, attribute: Attribute): OutputExpression = {
    if (expr.isEmpty) UnboundOutputExpression
    else BoundOutputExpression(expr, attribute)
  }
}
