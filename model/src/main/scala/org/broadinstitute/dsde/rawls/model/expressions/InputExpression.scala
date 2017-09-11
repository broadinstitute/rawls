package org.broadinstitute.dsde.rawls.model.expressions

import org.broadinstitute.dsde.rawls.model.{AttributeName, ErrorReportSource}
import org.broadinstitute.dsde.rawls.{RawlsException, StringValidationUtils}
import spray.json.JsValue

import scala.util.{Failure, Try}

sealed trait InputExpression
case class JSONInputExpression(json: JsValue) extends InputExpression {
  override def toString: String = json.compactPrint
}
case class TargetedInputExpression(target: ExpressionTarget, attributePath: Iterable[AttributeName]) extends InputExpression {
  override def toString: String = {
    val pathStr = attributePath map AttributeName.toDelimitedName mkString "."
    target.root + pathStr
  }
}

object TargetedInputExpression extends StringValidationUtils {
  override implicit val errorReportSource = ErrorReportSource("rawls")

  def tryParse(target: ExpressionTarget, expr: String): Try[TargetedInputExpression] = {
    if (expr.startsWith(target.root)) Try {
      val rawNames = expr.stripPrefix(target.root).split("\\.")
      if (rawNames.isEmpty || rawNames.exists(_.isEmpty)) throw new RawlsException(s"Invalid input expression: $expr")

      val attributeNames = rawNames map { name =>
        val attributeName = AttributeName.fromDelimitedName(name)
        validateUserDefinedString(attributeName.name)
        attributeName
      }
      TargetedInputExpression(target, attributeNames)
    }
    else Failure(new RawlsException(s"Invalid input expression: $expr"))
  }

  def build(expr: String): Try[TargetedInputExpression] = {
    tryParse(EntityTarget, expr) recoverWith { case _ => tryParse(WorkspaceTarget, expr) }
  }
}

object InputExpression {
  import spray.json._
  def build(expr: String): Try[InputExpression] = {
    Try { JSONInputExpression(expr.parseJson) } recoverWith  { case _ => TargetedInputExpression.build(expr) }
  }
}
