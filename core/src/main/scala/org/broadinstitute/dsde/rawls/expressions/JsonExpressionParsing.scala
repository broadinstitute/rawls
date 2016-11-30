package org.broadinstitute.dsde.rawls.expressions

import com.sun.xml.internal.ws.encoding.soap.DeserializationException
import org.broadinstitute.dsde.rawls.model._
import spray.json.JsonParser.ParsingException
import spray.json._

import scala.util.{Success, Try}

object JsonExpressionParsing {
  def evaluate(expression: String): Try[Attribute] = {
    try {
      val jsonExpr = expression.parseJson
      val attribute = WDLJsonSupport.attributeFormat.read(expression.parseJson)
      attribute match {
        case ref: AttributeEntityReference => Success(AttributeValueRawJson(jsonExpr))
        case refList: AttributeEntityReferenceList => Success(AttributeValueRawJson(jsonExpr))
      }
    } catch {
      case e: ParsingException => //jsonExpr was never json to begin with
      case e: DeserializationException =>
        Success(AttributeValueRawJson(jsonExpr))
        //could still be a mixed-type array or a jsobject that could be parsed as such. use raw json

    }

    /*
    * TODO: can we parse this as JSON?
    * if so, attempt to parse it through PlainArrayAttributeSerializer.read
    * - if that works, return the attribute
    * - watch out for objects that look like entity references: special-case them out, because they're lies
    * - otherwise return as a raw JSON attribute type if the parsing works but read() gives us a DeserializationException
    *
    * else fall through to expression parsing.
     */

    /*
    * TODO: ON TESTING
    * there are already tests for string and numeric literals. they should pass with absolutely no changes
    * add more tests for arrays-of-values: [1, 2, "three", true] is a valid JSON array that should make an AttributeValueList
    * other types such as maps and anything that is or includes an entity reference should return a AttributeRawJSON type instead
     */
  }
}
