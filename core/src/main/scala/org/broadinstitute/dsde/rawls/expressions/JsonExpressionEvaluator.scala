package org.broadinstitute.dsde.rawls.expressions

import org.broadinstitute.dsde.rawls.model._
import spray.json._

import scala.util.Try

object JsonExpressionEvaluator {
  def evaluate(expression: String): Try[Attribute] = {
    val jsonExprT: Try[JsValue] = Try(expression.parseJson)
    jsonExprT map { jsonExpr =>
      WDLJsonSupport.attributeFormat.read(expression.parseJson)
    } map {
      //handle the user typing in JSON that looks like our representation of references, which aren't legit WDL inputs.
      //turn it back into raw JSON.
      case _: AttributeEntityReference => AttributeValueRawJson(jsonExprT.get)
      case _: AttributeEntityReferenceList => AttributeValueRawJson(jsonExprT.get)

      //otherwise, we're cool
      case a: Attribute => a
    } recover {
      //DeserializationException will be thrown if the user gives us JSON that we fail to parse as one of our
      //Attribute types, but is still legit JSON. In this case we treat it as raw JSON, because it is.
      case _: DeserializationException => AttributeValueRawJson(jsonExprT.get)
    }
  }
}
