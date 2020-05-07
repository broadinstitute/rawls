package org.broadinstitute.dsde.rawls.expressions

import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, EntityRecord, ReadWriteAction}
import org.broadinstitute.dsde.rawls.model.AttributeValue

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

// a thin abstraction layer over SlickExpressionEvaluator

object ExpressionEvaluator {
  def withNewExpressionEvaluator[R](parser: DataAccess, rootEntities: Option[Seq[EntityRecord]])
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
}

class ExpressionEvaluator(slickEvaluator: SlickExpressionEvaluator, val rootEntities: Option[Seq[EntityRecord]]) {
  def evalFinalAttribute(workspaceContext: SlickWorkspaceContext, expression: String): ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]] = {
    import slickEvaluator.parser.driver.api._

    /*
      - parse expression using ANTLR parser
      - somehow find out the all the expressions in JSON that are attribute expression
      - for each attribute expression call slickEvaluator.evalFinalAttribute() and gather the results by running the DBIO actions
      - rebuild the JSON object (!) by replacing that value corresponding to expression in it

      so for expression: {"reference": {"bamFile":this.attrRef}} we need to send {"reference": {"bamFile":"gs://abc/123"}} to Cromwell

      or for expression: ["abc", "123", this.ref] -> ["abc", "123", "gs://abc/123"]
     */

    JsonExpressionEvaluator.evaluate(expression) match {
      //if the expression evals as JSON, it evaluates to the same thing for every entity, so build that map here
      case Success(parsed) => DBIO.successful(rootEntities match {
        case Some(entities) => (entities map { entityRec: EntityRecord =>
          entityRec.name -> Success(parsed)
        }).toMap
        case None => Map("" -> Success(parsed))
      })

      case Failure(_) => slickEvaluator.evalFinalAttribute(workspaceContext, expression)
    }
  }

  def evalFinalEntity(workspaceContext: SlickWorkspaceContext, expression:String): ReadWriteAction[Iterable[EntityRecord]] = {
    //entities have to be proper expressions, not JSON-y
    slickEvaluator.evalFinalEntity(workspaceContext, expression)
  }
}
