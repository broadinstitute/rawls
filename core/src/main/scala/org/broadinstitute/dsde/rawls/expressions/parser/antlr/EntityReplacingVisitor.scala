package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import org.broadinstitute.dsde.rawls.expressions.parser.antlr.ExtendedJSONParser.{PairContext, ValueContext}
import spray.json._

import scala.collection.JavaConverters._
import scala.collection.immutable.TreeMap

class EntityReplacingVisitor(lookupMap: Map[String, JsValue]) extends ExtendedJSONBaseVisitor[JsValue] {
  /**
    * {@inheritDoc }
    *
    * <p>The default implementation returns the result of calling
    * {@link #visitChildren} on {@code ctx}.</p>
    */
  override def visitObj(ctx: ExtendedJSONParser.ObjContext): JsValue = {
    val children: Map[String, JsValue] = ctx.getRuleContexts(classOf[PairContext]).asScala
      .map(visit)
      .map(_.asJsObject.fields)
      .reduceOption(_ ++ _)
      .getOrElse(TreeMap.empty)

    JsObject(children)
  }


  override def visitPair(ctx: ExtendedJSONParser.PairContext): JsValue = {
    val childKey = ctx.getChild(0) // STRING
    val childValue = ctx.getChild(2) // VALUE

    val abc = visit(childValue)
    val keyString = childKey.getText.parseJson match {
      case JsString(value) => value
      case other => ???
    }

    JsObject(keyString -> abc)
  }

  /**
    * {@inheritDoc }
    *
    * <p>The default implementation returns the result of calling
    * {@link #visitChildren} on {@code ctx}.</p>
    */
  override def visitArr(ctx: ExtendedJSONParser.ArrContext): JsValue = {
    JsArray(ctx.getRuleContexts(classOf[ValueContext]).asScala.map(visit).toVector)
  }

  override def visitLookup(ctx: ExtendedJSONParser.LookupContext): JsValue = {
    lookupMap(ctx.getText)
  }

  override def visitValue(ctx: ExtendedJSONParser.ValueContext): JsValue = {
    visit(ctx.getChild(0))
  }


  override def visitLiteral(ctx: ExtendedJSONParser.LiteralContext): JsValue = {
    ctx.getText.parseJson
  }
}
