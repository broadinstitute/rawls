package org.broadinstitute.dsde.rawls.model.expressions

import org.broadinstitute.dsde.rawls.{RawlsException, StringValidationUtils}
import org.broadinstitute.dsde.rawls.model.{Attribute, AttributeName, ErrorReportSource}

import scala.util.{Failure, Success, Try}

sealed trait OutputExpression
case object BlankOutputExpression extends OutputExpression {
  override def toString: String = ""
}
case class TargetedOutputExpression(target: ExpressionTarget, attributeName: AttributeName) extends OutputExpression {
  override def toString: String = s"${target.root}${AttributeName.toDelimitedName(attributeName)}"
}

sealed trait OutputExpressionBinding
case object UnboundOutputExpression extends OutputExpressionBinding
case class BoundOutputExpression(target: ExpressionTarget, attributeName: AttributeName, attribute: Attribute) extends OutputExpressionBinding

object TargetedOutputExpression extends StringValidationUtils {
  override implicit val errorReportSource = ErrorReportSource("rawls")

  def tryParse(target: ExpressionTarget, expr: String): Try[TargetedOutputExpression] = {
    if (expr.startsWith(target.root)) Try {
      val rawNames = expr.stripPrefix(target.root).split("\\.")
      if (rawNames.length != 1 || rawNames.exists(_.isEmpty)) throw new RawlsException(s"Invalid output expression: $expr")

      val attributeNames = rawNames map { name =>
        val attributeName = AttributeName.fromDelimitedName(name)
        validateUserDefinedString(attributeName.name)
        attributeName
      }

      TargetedOutputExpression(target, attributeNames.head)
    }
    else Failure(new RawlsException(s"Invalid output expression: $expr"))
  }

  def build(expr: String): Try[TargetedOutputExpression] = {
    tryParse(EntityTarget, expr) recoverWith { case _ => tryParse(WorkspaceTarget, expr) }
  }
}

object OutputExpression {
  def build(expr: String): Try[OutputExpression] = {
    if (expr.isEmpty) Success(BlankOutputExpression)
    else TargetedOutputExpression.build(expr)
  }

  def bind(outputExprStr: String, output: Attribute): Try[OutputExpressionBinding] = {
    build(outputExprStr) map {
      case targeted: TargetedOutputExpression => BoundOutputExpression(targeted.target, targeted.attributeName, output)
      case BlankOutputExpression => UnboundOutputExpression
    }
  }
}
