package org.broadinstitute.dsde.rawls.entities.datarepo

import org.broadinstitute.dsde.rawls.entities.base.ExpressionParser
import org.broadinstitute.dsde.rawls.model.AttributeName

import scala.concurrent.Future

class DataRepoExpressionParser extends ExpressionParser[Future, DataRepoExpressionContext, List[String]] {
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