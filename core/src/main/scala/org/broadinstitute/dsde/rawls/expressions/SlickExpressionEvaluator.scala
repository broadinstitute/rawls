package org.broadinstitute.dsde.rawls.expressions

import java.util.UUID

import _root_.slick.dbio
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.model._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

// accesible only via ExpressionEvaluator

private[expressions] object SlickExpressionEvaluator {
  def withNewExpressionEvaluator[R](parser: DataAccess, rootEntities: Seq[EntityRecord])
                                   (op: SlickExpressionEvaluator => ReadWriteAction[R])
                                   (implicit executionContext: ExecutionContext): ReadWriteAction[R] = {
    val evaluator = new SlickExpressionEvaluator(parser, rootEntities)

    evaluator.populateExprEvalScratchTable() andThen
      op(evaluator) andFinally
      evaluator.clearExprEvalScratchTable()
  }

  def withNewExpressionEvaluator[R](parser: DataAccess, workspaceContext: SlickWorkspaceContext, rootType: String, rootName: String)
                                   (op: SlickExpressionEvaluator => ReadWriteAction[R])
                                   (implicit executionContext: ExecutionContext): ReadWriteAction[R] = {
    import parser.driver.api._

    //Find the root entity for the expression
    val dbRootEntityRec = parser.entityQuery.findEntityByName(workspaceContext.workspaceId, rootType, rootName).result

    //Sanity check that we've only got one, and then pass upwards
    dbRootEntityRec flatMap { rootEntityRec =>
      if(rootEntityRec.size != 1) {
        DBIO.failed(new RawlsException(s"Expected 1 root entity type, found ${rootEntityRec.size} when searching for $rootType/$rootName"))
      } else {
        withNewExpressionEvaluator(parser, rootEntityRec)(op)
      }
    }
  }
}

private[expressions] class SlickExpressionEvaluator protected (val parser: DataAccess, val rootEntities: Seq[EntityRecord])(implicit executionContext: ExecutionContext) {
  import parser.driver.api._

  val transactionId = UUID.randomUUID().toString

  private def populateExprEvalScratchTable() = {
    val exprEvalBatches = rootEntities.map( e => ExprEvalRecord(e.id, e.name, transactionId) ).grouped(parser.batchSize)

    DBIO.sequence(exprEvalBatches.toSeq.map(batch => parser.exprEvalQuery ++= batch))
  }

  private def clearExprEvalScratchTable() = {
    parser.exprEvalQuery.filter(_.transactionId === transactionId).delete
  }

  def evalFinalAttribute(workspaceContext: SlickWorkspaceContext, expression: String): ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]] = {
    parser.parseAttributeExpr(expression, allowRootEntity = true) match {
      case Failure(regret) => DBIO.failed(new RawlsException(regret.getMessage))
      case Success(expr) =>
        runPipe(SlickExpressionContext(workspaceContext, rootEntities, transactionId), expr) map { exprResults =>
          val results = exprResults map { case (key, attrVals) =>
            key -> Try(attrVals.collect {
              case AttributeNull => Seq.empty
              case AttributeValueEmptyList => Seq.empty
              case av: AttributeValue => Seq(av)
              case avl: AttributeValueList => avl.list
              case ae: AttributeEntityReference => throw new RawlsException("Attribute expression returned a reference to an entity.")
              case ael: AttributeEntityReferenceList => throw new RawlsException("Attribute expression returned a list of entities.")
              case AttributeEntityReferenceEmptyList => throw new RawlsException("Attribute expression returned a list of entities.")
              case badType =>
                val message = s"unsupported type resulting from attribute expression: $badType: ${badType.getClass}"
                val MAX_ERROR_SIZE = 997
                val trimmed = if( message.length > MAX_ERROR_SIZE ) {
                  message.take(MAX_ERROR_SIZE) + "..."
                } else {
                  message
                }
                throw new RawlsException(trimmed)
            }.flatten)
          }
          //add any missing entities (i.e. those missing the attribute) back into the result map
          results ++ rootEntities.map(_.name).filterNot( results.keySet.contains ).map { missingKey => missingKey -> Success(Seq()) }
        }
    }
  }

  //This is boiling away the Try associated with attempting to parse the expression. Is this OK?
  def evalFinalEntity(workspaceContext: SlickWorkspaceContext, expression:String): ReadWriteAction[Iterable[EntityRecord]] = {
    if( rootEntities.isEmpty ) {
      DBIO.failed(new RawlsException(s"ExpressionEvaluator has no entities passed to evalFinalEntity $expression"))
    } else if( rootEntities.size > 1 ) {
      DBIO.failed(new RawlsException(s"ExpressionEvaluator has been set up with ${rootEntities.size} entities for evalFinalEntity, can only accept 1."))
    } else {
      parser.parseEntityExpr(expression) match {
        //fail out if we couldn't parse the expression
        case Failure(regret) => DBIO.failed(regret)
        case Success(expr) =>
          //If parsing succeeded, evaluate the expression using the given root entities and retype back to EntityRecord
          runPipe(SlickExpressionContext(workspaceContext, rootEntities, transactionId), expr).map { resultMap =>
            //NOTE: As per the DBIO.failed a few lines up, resultMap should only have one key, the same root elem.
            val (rootElem, elems) = resultMap.head
            elems.collect { case e: EntityRecord => e }
          }
      }
    }
  }

  private def runPipe(expressionContext: SlickExpressionContext, pipe: parser.PipelineQuery): ReadAction[Map[String, Iterable[Any]]] = {
    val builtPipe = pipe.rootStep.map(rootStep => pipe.steps.foldLeft(rootStep(expressionContext)){ ( queryPipeline, func ) => func(expressionContext, queryPipeline) })

    //Run the final step. This executes the pipeline and returns its output.
    Try {
      pipe.finalStep( expressionContext, builtPipe )
    } match {
      case Success(finalResult) => finalResult
      case Failure(regret) => dbio.DBIO.failed(regret)
    }
  }

}
