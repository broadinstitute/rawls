package org.broadinstitute.dsde.rawls.expressions

import com.sun.xml.internal.ws.encoding.soap.DeserializationException
import org.broadinstitute.dsde.rawls.model._
import spray.json.JsonParser.ParsingException
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
      case av: AttributeValue => Seq(av)
      case avl: AttributeValueList => avl.list
      case AttributeValueEmptyList => Seq.empty
    } recover {
      //DeserializationException will be thrown if the user gives us JSON that we fail to parse as one of our
      //Attribute types, but is still legit JSON. In this case we treat it as raw JSON, because it is.
      case _: DeserializationException => Seq(AttributeValueRawJson(jsonExprT.get))
    }

    //todo: if this is an array type, turn it into a seq of its elements
    //otherwise turn it into a seq of one

    /*
    * TODO: ON TESTING
    * there are already tests for string and numeric literals. they should pass with absolutely no changes
    * add more tests for arrays-of-values: [1, 2, "three", true] is a valid JSON array that should make an AttributeValueList
    * other types such as maps and anything that is or includes an entity reference should return a AttributeRawJSON type instead
     */
  }
}
