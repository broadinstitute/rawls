package org.broadinstitute.dsde.rawls.util

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, EntityRecord, ReadAction, ReadWriteAction}
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationContext
import org.broadinstitute.dsde.rawls.expressions.ExpressionEvaluator
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, Entity, ErrorReport, SlickWorkspaceContext}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

trait EntitySupport {
  implicit protected val executionContext: ExecutionContext
  protected val dataSource: SlickDataSource

  import dataSource.dataAccess.driver.api._

  //Finds a single entity record in the db.
  def withSingleEntityRec[T](entityType: String, entityName: String, workspaceContext: SlickWorkspaceContext, dataAccess: DataAccess)(op: (Seq[EntityRecord]) => ReadWriteAction[T]): ReadWriteAction[T] = {
    val entityRec = dataAccess.entityQuery.findEntityByName(workspaceContext.workspaceIdAsUUID, entityType, entityName).result
    entityRec flatMap { entities =>
      if (entities.isEmpty) {
        DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"No entity of type ${entityType} named ${entityName} exists in this workspace.")))
      } else if (entities.size == 1) {
        op(entities)
      } else {
        DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"More than one entity of type ${entityType} named ${entityName} exists in this workspace?!")))
      }
    }
  }

  def withAllEntities[T](workspaceContext: SlickWorkspaceContext, dataAccess: DataAccess, entities: Seq[AttributeEntityReference])(op: (Seq[Entity]) => ReadWriteAction[T]): ReadWriteAction[T] = {
    val entityActions: Seq[ReadAction[Try[Entity]]] = entities map { e =>
      dataAccess.entityQuery.get(workspaceContext, e.entityType, e.entityName) map {
        case None => Failure(new RawlsException(s"${e.entityType} ${e.entityName} does not exist in ${workspaceContext.workspace.toWorkspaceName}"))
        case Some(entity) => Success(entity)
      }
    }

    DBIO.sequence(entityActions) flatMap { entityTries =>
      val failures = entityTries.collect { case Failure(y) => y.getMessage }
      if (failures.isEmpty) op(entityTries collect { case Success(e) => e })
      else {
        val err = ErrorReport(statusCode = StatusCodes.BadRequest, message = (Seq("Entities were not found:") ++ failures) mkString System.lineSeparator())
        DBIO.failed(new RawlsExceptionWithErrorReport(err))
      }
    }
  }

  def withEntityRecsForExpressionEval[T](expressionEvaluationContext: ExpressionEvaluationContext, workspaceContext: SlickWorkspaceContext, dataAccess: DataAccess)(op: (Option[Seq[EntityRecord]]) => ReadWriteAction[T]): ReadWriteAction[T] = {
    if( expressionEvaluationContext.rootEntityType.isEmpty ) {
      op(None)
    } else {
      val rootEntityType = expressionEvaluationContext.rootEntityType.get

      //If there's an expression, evaluate it to get the list of entities to run this job on.
      //Otherwise, use the entity given in the submission.
      expressionEvaluationContext.expression match {
        case None =>
          if (expressionEvaluationContext.entityType.getOrElse("") != rootEntityType) {
            val whatYouGaveUs = if (expressionEvaluationContext.entityType.isDefined) s"an entity of type ${expressionEvaluationContext.entityType.get}" else "no entity"
            DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"Method configuration expects an entity of type $rootEntityType, but you gave us $whatYouGaveUs.")))
          } else {
            withSingleEntityRec(expressionEvaluationContext.entityType.get, expressionEvaluationContext.entityName.get, workspaceContext, dataAccess)(rec => op(Some(rec)))
          }
        case Some(expression) =>
          ExpressionEvaluator.withNewExpressionEvaluator(dataAccess, workspaceContext, expressionEvaluationContext.entityType.get, expressionEvaluationContext.entityName.get) { evaluator =>
            evaluator.evalFinalEntity(workspaceContext, expression).asTry
          } flatMap { //gotta close out the expression evaluator to wipe the EXPREVAL_TEMP table
            case Failure(regret) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, regret)))
            case Success(entityRecords) =>
              if (entityRecords.isEmpty) {
                DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, "No entities eligible for submission were found.")))
              } else {
                val eligibleEntities = entityRecords.filter(_.entityType == rootEntityType).toSeq
                if (eligibleEntities.isEmpty)
                  DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"The expression in your SubmissionRequest matched only entities of the wrong type. (Expected type ${rootEntityType}.)")))
                else
                  op(Some(eligibleEntities))
              }
          }
      }
    }
  }
}
