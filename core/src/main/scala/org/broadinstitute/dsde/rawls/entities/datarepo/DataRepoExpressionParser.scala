package org.broadinstitute.dsde.rawls.entities.datarepo

import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.entities.base.ExpressionParser
import org.broadinstitute.dsde.rawls.model.{AttributeName, ErrorReport}

import scala.concurrent.Future
import scala.util.Try

class DataRepoExpressionParser extends ExpressionParser[Future, DataRepoExpressionContext, List[String]] {
  /** I think something along these lines would be sufficient for use in parseMCExpressions in ExpressionParser trait
   BUT it should be noted that I don't personally fully understand the syntax rules for expressions and we'd need to
   make sure that our parsing lived up to the syntactic checking performed by the local expression parser... or at least
   accept that we might say expressions are valid that really aren't OR end up throwing exceptions later if we can't map
   it to the schema. Maybe that's fine since the schema comparison gives us a bit of extra safety -- even if we get
   the expression syntax parsing a bit wrong, it won't be considered valid unless it can map to the schema (and is it even wrong then?)  */
  override protected def parseInputExpr(allowRootEntity: Boolean)(expression: String): Try[Unit] = Try {
    val parsed = expression.split('.')
    if (!parsed.headOption.contains("this") || parsed.contains("")) throw new RawlsExceptionWithErrorReport(ErrorReport("terrible input expression -- try again"))
  }

  override protected def parseOutputExpr(allowRootEntity: Boolean)(expression: String): Try[Unit] = ???

  override def parseAttributeExpr(expression: String, allowRootEntity: Boolean): Try[PipelineQuery] = ???

  // todo: do we still need the following functions if we're overriding these functions further up the chain?
  /** my answer to this question is yes, I believe we'll still need these functions in order to generate the BQ queries
    * needed to evaluate the expressions and grab the intended value */

  override protected def entityRootFunc(context: DataRepoExpressionContext): List[String] = ???

  override protected def workspaceAttributeFinalFunc(attrName: AttributeName)(context: DataRepoExpressionContext, shouldBeNone: Option[List[String]]): ExpressionOutputType = ???

  override protected def workspaceEntityRefRootFunc(attrName: AttributeName)(context: DataRepoExpressionContext): List[String] = ???

  override protected def entityNameAttributePipeFunc(attrName: AttributeName)(context: DataRepoExpressionContext, queryPipeline: List[String]): List[String] = ???

  override protected def entityAttributeFinalFunc(attrName: AttributeName)(context: DataRepoExpressionContext, queryPipeline: Option[List[String]]): ExpressionOutputType = ???

  override protected def entityReservedAttributeFinalFunc(attributeName: String)(context: DataRepoExpressionContext, queryPipeline: Option[List[String]]): ExpressionOutputType = ???

  override protected def workspaceReservedAttributeFinalFunc(attributeName: String)(context: DataRepoExpressionContext, shouldBeNone: Option[List[String]]): ExpressionOutputType = ???

  override protected def entityFinalFunc(context: DataRepoExpressionContext, queryPipeline: Option[List[String]]): ExpressionOutputType = ???

  override protected def workspaceEntityFinalFunc(attrName: AttributeName)(context: DataRepoExpressionContext, shouldBeNone: Option[List[String]]): ExpressionOutputType = ???
}

case class DataRepoExpressionContext()