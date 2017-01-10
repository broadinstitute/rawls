package org.broadinstitute.dsde.rawls.expressions

import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, EntityRecord, ReadWriteAction}
import org.broadinstitute.dsde.rawls.model.AttributeValue

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

object ExpressionEvaluator {
  def withNewExpressionEvaluator[R](parser: DataAccess, rootEntities: Seq[EntityRecord])
                                   (op: ExpressionEvaluator => ReadWriteAction[R])
                                   (implicit executionContext: ExecutionContext): ReadWriteAction[R] = {

    SlickExpressionEvaluator.withNewExpressionEvaluator(parser, rootEntities) { slickEvaluator =>
      op(new ExpressionEvaluator(slickEvaluator, slickEvaluator.rootEntities))
    }
  }

  def withNewExpressionEvaluator[R](parser: DataAccess, workspaceContext: SlickWorkspaceContext, rootType: String, rootName: String)
                                   (op: ExpressionEvaluator => ReadWriteAction[R])
                                   (implicit executionContext: ExecutionContext): ReadWriteAction[R] = {

    SlickExpressionEvaluator.withNewExpressionEvaluator(parser, workspaceContext, rootType, rootName) { slickEvaluator =>
      op(new ExpressionEvaluator(slickEvaluator, slickEvaluator.rootEntities))
    }
  }

  def validateAttributeExpr(parser: DataAccess)(expression: String): Try[Boolean] = {
    JsonExpressionParsing.evaluate(expression) match {
      case Success(parsed) => Success(true)
      case Failure(regret) => parser.parseAttributeExpr(expression) map (_ => true)
    }
  }

  def validateOutputExpr(parser: DataAccess)(expression: String): Try[Boolean] = {
    parser.parseOutputExpr(expression) map (_ => true)
  }
}

class ExpressionEvaluator(slickEvaluator: SlickExpressionEvaluator, val rootEntities: Seq[EntityRecord]) {
  def evalFinalAttribute(workspaceContext: SlickWorkspaceContext, expression: String): ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]] = {
    import slickEvaluator.parser.driver.api._

    JsonExpressionParsing.evaluate(expression) match {
      //if the expression evals as JSON, it evaluates to the same thing for every entity, so build that map here
      case Success(parsed) => DBIO.successful((rootEntities map { entityRec: EntityRecord =>
        entityRec.name -> Success(parsed)
      }).toMap)

      case Failure(regret) => slickEvaluator.evalFinalAttribute(workspaceContext, expression)
    }
  }

  def evalFinalEntity(workspaceContext: SlickWorkspaceContext, expression:String): ReadWriteAction[Iterable[EntityRecord]] = {
    //entities have to be proper expressions, not JSON-y
    slickEvaluator.evalFinalEntity(workspaceContext, expression)
  }
}
