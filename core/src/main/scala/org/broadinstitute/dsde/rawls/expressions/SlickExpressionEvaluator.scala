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

  private def liftToRawJson(attrs: Iterable[Attribute]): AttributeValueRawJson = {
    import spray.json._
    import org.broadinstitute.dsde.rawls.model.WDLJsonSupport
    AttributeValueRawJson( JsArray( attrs.map( a => a.toJson(WDLJsonSupport.attributeFormat)).toVector ) )
  }

  def evalFinalAttribute(workspaceContext: SlickWorkspaceContext, expression: String): ReadWriteAction[Map[String, Try[Attribute]]] = {
    parser.parseAttributeExpr(expression) match {
      case Failure(regret) => DBIO.failed(new RawlsException(regret.getMessage))
      case Success(pipelineQuery) =>
        val pipeResults = runPipe[parser.AttributeResult](SlickExpressionContext(workspaceContext, rootEntities, transactionId), pipelineQuery)
        pipeResults map { (exprResults: Map[String, parser.AttributeResult]) =>
          val results: Map[String, Try[Attribute]] = exprResults map { case (rootEntityName: String, attrResult: parser.AttributeResult) =>

            // Lift the values out of the map and into a list -- the entity names aren't useful to us (but are likely to be in future).
            // We need to sort the results because some of the db queries run in the parser don't preserve order.
            // This list contains one Attribute (which may itself be an AttributeValueList) for each final entity in the evaluated expression.
            val finalAttributes = attrResult.lastEntityToAttribute.toList.sortWith { case ((entityName1, _), (entityName2, _)) =>
              entityName1 < entityName2
            }.map(_._2)

            //Unpack the iterable into the correct AttributeValue type.
            rootEntityName -> Try(finalAttributes match {
              //PEBKAC cases -- expression references an entity, not an attribute.
              case _ if finalAttributes.exists( _.isInstanceOf[AttributeEntityReference] ) => throw new RawlsException("Attribute expression returned a reference to an entity.")
              case _ if finalAttributes.exists( _.isInstanceOf[AttributeEntityReferenceList] ) => throw new RawlsException("Attribute expression returned a list of entities.")
              case _ if finalAttributes.contains( AttributeEntityReferenceEmptyList ) => throw new RawlsException("Attribute expression returned a list of entities.")

              //I don't think this is possible -- we only populate the map with entities who have values, and even nulls are values.
              case Nil => AttributeNull

              //unwrap single-element lists if we didn't traverse a list type
              //this case has to come before case _ :: _ as _ :: _ will indeed match _ :: Nil
              case a :: Nil if !attrResult.hasExplodedEntity => a

              //Something has gone VERY wrong if this happens.
              case _ :: _ if !attrResult.hasExplodedEntity =>
                throw new RawlsException("Attribute expression didn't traverse an entity list, yet returned more than one entity's worth of attributes???")

              //otherwise, lift the list into an AttributeValueList if we can
              case _ if finalAttributes.forall( _.isInstanceOf[AttributeValue] ) => AttributeValueList(finalAttributes.asInstanceOf[List[AttributeValue]])

              //if it's a 2D array, we have to lift it to raw json
              case _ if finalAttributes.contains( AttributeValueEmptyList ) => liftToRawJson(finalAttributes)
              case _ if finalAttributes.exists( _.isInstanceOf[AttributeValueList] ) => liftToRawJson(finalAttributes)

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
          }
          //add any missing entities (i.e. those missing the attribute) back into the result map
          results ++ rootEntities.map(_.name).filterNot( results.keySet.contains ).map { missingKey => missingKey -> Success(AttributeNull) }
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
          runPipe[parser.EntityResult](SlickExpressionContext(workspaceContext, rootEntities, transactionId), pipelineQuery).map { resultMap =>
            //NOTE: As per the DBIO.failed a few lines up, resultMap should only have one key, the same root elem.
            val (rootElem, elems) = resultMap.head
            elems.collect { case e: EntityRecord => e }
          }
      }
    }
  }

  /* Runs the pipe and returns its result.
   * The type parameter T here is either Iterable[EntityRecord] (for entity expressions) or Map[String, Attribute] (for attribute expressions).
   */
  private def runPipe[T](expressionContext: SlickExpressionContext, pipe: parser.PipelineQuery[parser.FinalResult[T]]): parser.FinalResult[T] = {
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
