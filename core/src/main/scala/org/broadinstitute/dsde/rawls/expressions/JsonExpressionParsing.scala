package org.broadinstitute.dsde.rawls.expressions

import com.sun.xml.internal.ws.encoding.soap.DeserializationException
import org.broadinstitute.dsde.rawls.model._
import spray.json._

import scala.util.{Success, Try}

object JsonExpressionParsing {
  def evaluate(expression: String): Try[Iterable[AttributeValue]] = {
    val jsonExprT = Try(expression.parseJson)
    jsonExprT map { jsonExpr =>
      WDLJsonSupport.attributeFormat.read(expression.parseJson)
    } map {
      //handle the user typing in JSON that looks like our representation of references, which aren't legit WDL inputs.
      //turn it back into raw JSON.
      case _: AttributeEntityReference => Seq(AttributeValueRawJson(jsonExprT.get))
      case _: AttributeEntityReferenceList => Seq(AttributeValueRawJson(jsonExprT.get))
      case AttributeNull => Seq.empty
      case av: AttributeValue => Seq(av)
      case avl: AttributeValueList => avl.list
      case AttributeValueEmptyList => Seq.empty
    } recover {
      //DeserializationException will be thrown if the user gives us JSON that we fail to parse as one of our
      //Attribute types, but is still legit JSON. In this case we treat it as raw JSON, because it is.
      case _: DeserializationException => Seq(AttributeValueRawJson(jsonExprT.get))
    }
  }
}
