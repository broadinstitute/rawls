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
        DBIO.failed(new RawlsException(s"Found != 1 root entity when searching for ${rootType}/$rootName"))
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

  def evalFinalAttribute(workspaceContext: SlickWorkspaceContext, expression: String): ReadWriteAction[Map[String, Try[Iterable[Attribute]]]] = {
    parser.parseAttributeExpr(expression) match {
      case Failure(regret) => DBIO.failed(new RawlsException(regret.getMessage))
      case Success(pipelineQuery) =>
        runPipe(SlickExpressionContext(workspaceContext, rootEntities, transactionId), pipelineQuery) map { (exprResults: Map[String, Iterable[Attribute]]) =>
          val results: Map[String, Try[Attribute]] = exprResults map { case (key: String, attrVals: Iterable[Attribute]) =>
            //In the case of this.participants.boo, attrVals might be [ [1,2,3], [4,5,6], "bees" ] if the participants have different types on "boo"

            /* THINGS WE KNOW:
              - if attrVals.size == 0 -- TODO: dunno? maybe c'est impossible.
              - if attrVals.size == 1, there was no intermediate ref-array in this expression (i.e. not a set type). this is the normal case
                  - if .head is an AttributeValue, all cool
                  - if .head is an AttributeValueList of any size (empty or otherwise), the last element was a list
                  - if .head is a Ref or a RefList, ya dun goofed
              - if attrVals.size > 1, there was an intermediate ref-array
                  - if all elems are AttributeValue, you can return a single AttributeValueList
                  - if any elem is an AttributeValueList, lift the entire thing into JSON and store that
                  - if any elem is a Ref or a RefList, ya dun goofed
             */


            val boop: Try[Iterable[Attribute]] = Try(attrVals.collect {
              case AttributeNull => AttributeNull
              case AttributeValueEmptyList => AttributeValueEmptyList
              case av: AttributeValue => av
              case avl: AttributeValueList => avl
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
            })
            key -> boop.flatten // NO! DON'T FLATTEN!! BAD RAWLS!!!
            Map.empty[String, Try[Attribute]]
          }.toMap
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
        case Success(pipelineQuery) =>
          //If parsing succeeded, evaluate the expression using the given root entities and retype back to EntityRecord
          runPipe(SlickExpressionContext(workspaceContext, rootEntities, transactionId), pipelineQuery).map { resultMap =>
            //NOTE: As per the DBIO.failed a few lines up, resultMap should only have one key, the same root elem.
            val (rootElem, elems) = resultMap.head
            elems.collect { case e: EntityRecord => e }
          }
      }
    }
  }

  /* Runs the pipe and returns its result.
   * The type parameter T here is either EntityRecord (for entity expressions) or Attribute (for attribute expressions).
   */
  private def runPipe[T](expressionContext: SlickExpressionContext, pipe: parser.PipelineQuery[parser.FinalResult[T]]): ReadAction[Map[String, Iterable[T]]] = {
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
