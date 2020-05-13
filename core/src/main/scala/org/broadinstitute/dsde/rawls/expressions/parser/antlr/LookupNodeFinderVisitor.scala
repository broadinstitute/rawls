package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import org.broadinstitute.dsde.rawls.expressions.parser.antlr.ExtendedJSONParser.LookupContext

class LookupNodeFinderVisitor extends ExtendedJSONBaseVisitor[Set[String]]{

  override def defaultResult(): Set[String] = Set.empty[String]

  override def aggregateResult(aggregate: Set[String], nextResult: Set[String]): Set[String] = {
    aggregate ++ nextResult
  }

  override def visitLookup(ctx: LookupContext): Set[String] = {
    Set(ctx.getText)
  }
}
