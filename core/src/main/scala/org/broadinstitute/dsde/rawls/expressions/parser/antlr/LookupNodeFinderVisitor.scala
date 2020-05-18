package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import org.broadinstitute.dsde.rawls.expressions.parser.antlr.ExtendedJSONParser.LookupContext

class LookupNodeFinderVisitor extends ExtendedJSONBaseVisitor[Set[String]]{

  /**
    * Gets the default value returned by visitor methods. The base implementation returns null.
    *
    * @return The default value returned by visitor methods
    */
  override def defaultResult() = Set.empty[String]

  /**
    * Aggregates the results of visiting multiple children of a node.
    *
    * @param aggregate The previous aggregate value. In the default implementation, the aggregate value is initialized to
    *                  {defaultResult()}, which is passed as the {aggregate} argument to this method after the first
    *                  child node is visited.
    * @param nextResult The result of the immediately preceeding call to visit a child node.
    * @return aggregated set of lookup expressions
    */
  override def aggregateResult(aggregate: Set[String], nextResult: Set[String]): Set[String] = aggregate ++ nextResult

  /**
    * Visit a parse tree produced by ExtendedJSONParser#lookup
    *
    * @param ctx the parse tree
    * @return the visitor result i.e. the lookup expression
    */
  override def visitLookup(ctx: LookupContext): Set[String] = Set(ctx.getText)
}
