package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.ExtendedJSONParser.{PairContext, ValueContext}
import spray.json._

import scala.collection.JavaConverters._

class ReconstructExpressionVisitor(lookupMap: Map[String, JsValue]) extends ExtendedJSONBaseVisitor[JsValue] {

  override def visitRoot(ctx: ExtendedJSONParser.RootContext): JsValue = {
    // ROOT rule always has 1 child
    visit(ctx.getChild(0))
  }

  override def visitObj(ctx: ExtendedJSONParser.ObjContext): JsValue = {
    ctx.getRuleContexts(classOf[PairContext]).asScala // get all children that are pairs
      .map(visit) // visitPair returns each pair as JsObject
      .map(_.asJsObject.fields)
      .reduceOption(_ ++ _) // duplicate keys will be lost. This should be ok as per: https://stackoverflow.com/questions/21832701/does-json-syntax-allow-duplicate-keys-in-an-object
      .map(JsObject.apply)
      .getOrElse(JsObject.empty)
  }

  /**
    * Visit the VALUE node child of a PAIR node, and return the results back as a JsObject to make it easier
    * to combine pairs while visiting OBJ node
    */
  override def visitPair(ctx: ExtendedJSONParser.PairContext): JsValue = {
    // PAIR has 3 children: STRING, COLON and VALUE
    val childKey = ctx.getChild(0) // STRING
    val childValue = visit(ctx.getChild(2)) // VALUE

    /*
       This returns the key in quotes, so the pair output will look like {"\"key\"":"value"}. Hence we need
       to get the unquoted string
     */
    val quotedKeyString = childKey.getText

    val unquotedKeyString = quotedKeyString.parseJson match {
      case JsString(value) => value
      case other =>
        // PAIR rule expects the key to be a STRING
        val token = ctx.getStart
        val errorMsg = if (token == null) "" else s"Offending symbol is on line ${token.getLine} at position ${token.getCharPositionInLine}."
        throw new RawlsException(s"Error while parsing the expression. Pair key `$quotedKeyString` should be STRING. Found $other. $errorMsg")
    }

    JsObject(unquotedKeyString -> childValue)
  }

  override def visitArr(ctx: ExtendedJSONParser.ArrContext): JsValue = {
    JsArray(ctx.getRuleContexts(classOf[ValueContext]).asScala.map(visit).toVector)
  }

  override def visitLookup(ctx: ExtendedJSONParser.LookupContext): JsValue = lookupMap(ctx.getText)

  override def visitValue(ctx: ExtendedJSONParser.ValueContext): JsValue = {
    // VALUE rule always has 1 child
    visit(ctx.getChild(0))
  }

  override def visitLiteral(ctx: ExtendedJSONParser.LiteralContext): JsValue = ctx.getText.parseJson
}
