package org.broadinstitute.dsde.rawls.entities

import akka.NotUsed
import akka.http.scaladsl.model.StatusCodes
import akka.stream.scaladsl.Source
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.cloud.bigquery.BigQueryException
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, EntityAndAttributesResult, ReadAction}
import org.broadinstitute.dsde.rawls.dataaccess.{AttributeTempTableType, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.entities.exceptions.{
  DataEntityException,
  DeleteEntitiesConflictException,
  DeleteEntitiesOfTypeConflictException,
  EntityNotFoundException
}
import org.broadinstitute.dsde.rawls.expressions.ExpressionEvaluator
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AttributeUpdateOperation, EntityUpdateDefinition}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.{
  AttributeSupport,
  AttributeUpdateOperationException,
  EntitySupport,
  JsonFilterUtils,
  WorkspaceSupport
}
import org.broadinstitute.dsde.rawls.workspace.WorkspaceRepository
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport, StringValidationUtils}
import slick.jdbc.{ResultSetConcurrency, ResultSetType, TransactionIsolation}

import java.sql.SQLException
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object EntityService {
  def constructor(dataSource: SlickDataSource,
                  samDAO: SamDAO,
                  workbenchMetricBaseName: String,
                  entityManager: EntityManager,
                  pageSizeLimit: Int
  )(ctx: RawlsRequestContext)(implicit executionContext: ExecutionContext): EntityService =
    new EntityService(ctx, dataSource, samDAO, entityManager, workbenchMetricBaseName, pageSizeLimit)
}

class EntityService(protected val ctx: RawlsRequestContext,
                    val dataSource: SlickDataSource,
                    val samDAO: SamDAO,
                    entityManager: EntityManager,
                    override val workbenchMetricBaseName: String,
                    pageSizeLimit: Int
)(implicit protected val executionContext: ExecutionContext)
    extends WorkspaceSupport
    with EntitySupport
    with AttributeSupport
    with LazyLogging
    with RawlsInstrumented
    with JsonFilterUtils
    with StringValidationUtils {

  import dataSource.dataAccess.driver.api._
  implicit override val errorReportSource: ErrorReportSource = ErrorReportSource("rawls")

  // used by WorkspaceSupport - in future refactoring, this can be moved into the constructor for better mocking
  val workspaceRepository: WorkspaceRepository = new WorkspaceRepository(dataSource)

  def createEntity(workspaceName: WorkspaceName, entity: Entity): Future[Entity] =
    withAttributeNamespaceCheck(entity) {
      for {
        workspaceContext <- getV2WorkspaceContextAndPermissions(workspaceName,
                                                                SamWorkspaceActions.write,
                                                                Some(WorkspaceAttributeSpecs(all = false))
        )
        entityManager <- entityManager.resolveProviderFuture(EntityRequestArguments(workspaceContext, ctx))
        result <- entityManager.createEntity(entity)
      } yield result
    }.recover(sqlLoggingRecover(s"createEntity: $workspaceName"))

  def getEntity(workspaceName: WorkspaceName,
                entityType: String,
                entityName: String,
                dataReference: Option[DataReferenceName],
                billingProject: Option[GoogleProjectId]
  ): Future[Entity] =
    getV2WorkspaceContextAndPermissions(workspaceName,
                                        SamWorkspaceActions.read,
                                        Some(WorkspaceAttributeSpecs(all = false))
    ) flatMap { workspaceContext =>
      val entityRequestArguments = EntityRequestArguments(workspaceContext, ctx, dataReference, billingProject)

      val entityFuture = for {
        entityProvider <- entityManager.resolveProviderFuture(entityRequestArguments)
        entity <- entityProvider.getEntity(entityType, entityName)
      } yield entity

      entityFuture
        .recover { case _: EntityNotFoundException =>
          // could move this error message into EntityNotFoundException and allow it to bubble up
          throw new RawlsExceptionWithErrorReport(
            ErrorReport(StatusCodes.NotFound, s"${entityType} ${entityName} does not exist in $workspaceName")
          )
        }
        .recover(sqlLoggingRecover(s"getEntity: $workspaceName $entityType/$entityName"))
        .recover(bigQueryRecover)
    }

  def updateEntity(workspaceName: WorkspaceName,
                   entityType: String,
                   entityName: String,
                   operations: Seq[AttributeUpdateOperation]
  ): Future[Entity] =
    withAttributeNamespaceCheck(operations.map(_.name)) {
      for {
        workspaceContext <- getV2WorkspaceContextAndPermissions(workspaceName,
                                                                SamWorkspaceActions.write,
                                                                Some(WorkspaceAttributeSpecs(all = false))
        )
        entityProvider <- entityManager.resolveProviderFuture(EntityRequestArguments(workspaceContext, ctx))
        result <- entityProvider.updateEntity(entityType, entityName, operations)
      } yield result
    }.recover(sqlLoggingRecover(s"updateEntity: $workspaceName $entityType/$entityName ${operations.size} operations"))

  def deleteEntities(workspaceName: WorkspaceName,
                     entRefs: Seq[AttributeEntityReference],
                     dataReference: Option[DataReferenceName],
                     billingProject: Option[GoogleProjectId]
  ): Future[Set[AttributeEntityReference]] =
    // short-circuit: if caller requested to delete nothing, then we do nothing
    if (entRefs.isEmpty) {
      Future.successful(Set.empty)
    } else {
      getV2WorkspaceContextAndPermissions(workspaceName,
                                          SamWorkspaceActions.write,
                                          Some(WorkspaceAttributeSpecs(all = false))
      ) flatMap { workspaceContext =>
        val entityRequestArguments = EntityRequestArguments(workspaceContext, ctx, dataReference, billingProject)

        val deleteFuture = for {
          entityProvider <- entityManager.resolveProviderFuture(entityRequestArguments)
          _ <- entityProvider.deleteEntities(entRefs)
        } yield Set[AttributeEntityReference]()

        deleteFuture
          .recover { case delEx: DeleteEntitiesConflictException =>
            delEx.referringEntities
          }
          .recover(sqlLoggingRecover(s"deleteEntities: $workspaceName ${entRefs.size} entities"))
          .recover(bigQueryRecover)
      }
    }

  def deleteEntitiesOfType(workspaceName: WorkspaceName,
                           entityType: String,
                           dataReference: Option[DataReferenceName],
                           billingProject: Option[GoogleProjectId]
  ) =
    getV2WorkspaceContextAndPermissions(workspaceName,
                                        SamWorkspaceActions.write,
                                        Some(WorkspaceAttributeSpecs(all = false))
    ) flatMap { workspaceContext =>
      val entityRequestArguments = EntityRequestArguments(workspaceContext, ctx, dataReference, billingProject)

      val deleteFuture = for {
        entityProvider <- entityManager.resolveProviderFuture(entityRequestArguments)
        numberOfEntitiesDeleted <- entityProvider.deleteEntitiesOfType(entityType)
      } yield numberOfEntitiesDeleted

      deleteFuture
        .recover { case delEx: DeleteEntitiesOfTypeConflictException =>
          throw new RawlsExceptionWithErrorReport(
            ErrorReport(
              StatusCodes.Conflict,
              s"Entity type [$entityType] cannot be deleted because there are ${delEx.conflictCount} references " +
                s"to this entity type. All references must be removed before deleting a type."
            )
          )
        }
        .recover(sqlLoggingRecover(s"deleteEntitiesOfType: $workspaceName $entityType"))
        .recover(bigQueryRecover)
    }

  def deleteEntityAttributes(workspaceName: WorkspaceName,
                             entityType: String,
                             attributeNames: Set[AttributeName]
  ): Future[Unit] =
    (for {
      workspaceContext <- getV2WorkspaceContextAndPermissions(workspaceName,
                                                              SamWorkspaceActions.write,
                                                              Some(WorkspaceAttributeSpecs(all = false))
      )
      entityProvider <- entityManager.resolveProviderFuture(EntityRequestArguments(workspaceContext, ctx))
      result <- entityProvider.deleteEntityAttributes(entityType, attributeNames)
    } yield result)
      .recover(
        sqlLoggingRecover(s"deleteEntityAttributes: $workspaceName $entityType ${attributeNames.size} attribute names")
      )

  def renameEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, newName: String): Future[Int] =
    (for {
      workspaceContext <- getV2WorkspaceContextAndPermissions(workspaceName,
                                                              SamWorkspaceActions.write,
                                                              Some(WorkspaceAttributeSpecs(all = false))
      )
      entityProvider <- entityManager.resolveProviderFuture(EntityRequestArguments(workspaceContext, ctx))
      result <- entityProvider.renameEntity(entityType, entityName, newName)
    } yield result).recover(
      sqlLoggingRecover(s"renameEntity: $workspaceName $entityType $entityName")
    )

  def renameEntityType(workspaceName: WorkspaceName, oldName: String, renameInfo: EntityTypeRename): Future[Int] = {
    validateEntityType(renameInfo.newName)
    (for {
      workspaceContext <- getV2WorkspaceContextAndPermissions(workspaceName,
                                                              SamWorkspaceActions.write,
                                                              Some(WorkspaceAttributeSpecs(all = false))
      )
      entityProvider <- entityManager.resolveProviderFuture(EntityRequestArguments(workspaceContext, ctx))
      result <- entityProvider.renameEntityType(oldName, renameInfo)
    } yield result).recover(
      sqlLoggingRecover(s"renameEntityType: $workspaceName $workspaceName $oldName")
    )
  }

  def evaluateExpression(workspaceName: WorkspaceName,
                         entityType: String,
                         entityName: String,
                         expression: String
  ): Future[Seq[AttributeValue]] =
    (for {
      workspaceContext <- getV2WorkspaceContextAndPermissions(workspaceName,
                                                              SamWorkspaceActions.read,
                                                              Some(WorkspaceAttributeSpecs(all = false))
      )
      entityProvider <- entityManager.resolveProviderFuture(EntityRequestArguments(workspaceContext, ctx))
      result <- entityProvider.evaluateExpression(entityType, entityName, expression)
    } yield result).recover(
      sqlLoggingRecover(s"evaluateExpression: $workspaceName $entityType $entityName $expression")
    )

  def entityTypeMetadata(workspaceName: WorkspaceName,
                         dataReference: Option[DataReferenceName],
                         billingProject: Option[GoogleProjectId],
                         useCache: Boolean
  ): Future[Map[String, EntityTypeMetadata]] =
    (getV2WorkspaceContextAndPermissions(workspaceName,
                                         SamWorkspaceActions.read,
                                         Some(WorkspaceAttributeSpecs(all = false))
    ) flatMap { workspaceContext =>
      val entityRequestArguments = EntityRequestArguments(workspaceContext, ctx, dataReference, billingProject)

      val metadataFuture = for {
        entityProvider <- entityManager.resolveProviderFuture(entityRequestArguments)
        metadata <- entityProvider.entityTypeMetadata(useCache)
      } yield metadata

      metadataFuture.recover(bigQueryRecover)
    }).recover(
      sqlLoggingRecover(s"entityTypeMetadata: $workspaceName")
    )

  def listEntities(workspaceName: WorkspaceName, entityType: String): Future[Source[Entity, NotUsed]] =
    (for {
      workspaceContext <- getV2WorkspaceContextAndPermissions(workspaceName,
                                                              SamWorkspaceActions.read,
                                                              Some(WorkspaceAttributeSpecs(all = false))
      )
      entityProvider <- entityManager.resolveProviderFuture(EntityRequestArguments(workspaceContext, ctx))
      result = entityProvider.listEntities(entityType)
    } yield result).recover(
      sqlLoggingRecover(s"listEntities: $workspaceName $entityType")
    )

  def queryEntitiesSource(workspaceName: WorkspaceName,
                          dataReference: Option[DataReferenceName],
                          entityType: String,
                          query: EntityQuery,
                          billingProject: Option[GoogleProjectId]
  ): Future[(EntityQueryResultMetadata, Source[Entity, _])] = {
    if (query.pageSize > pageSizeLimit) {
      throw new RawlsExceptionWithErrorReport(
        ErrorReport(StatusCodes.BadRequest, s"Page size cannot exceed $pageSizeLimit")
      )
    }

    getV2WorkspaceContextAndPermissions(workspaceName,
                                        SamWorkspaceActions.read,
                                        Some(WorkspaceAttributeSpecs(all = false))
    ) flatMap { workspaceContext =>
      val entityRequestArguments = EntityRequestArguments(workspaceContext, ctx, dataReference, billingProject)

      val queryFuture = for {
        entityProvider <- entityManager.resolveProviderFuture(entityRequestArguments)
        metadataAndEntitySource <- entityProvider.queryEntitiesSource(entityType, query, ctx)
      } yield metadataAndEntitySource

      queryFuture.recover(bigQueryRecover)
    }
  }

  def copyEntities(entityCopyDef: EntityCopyDefinition, linkExistingEntities: Boolean): Future[EntityCopyResponse] =
    (for {
      destWsCtx <- getV2WorkspaceContextAndPermissions(entityCopyDef.destinationWorkspace,
                                                       SamWorkspaceActions.write,
                                                       Some(WorkspaceAttributeSpecs(all = false))
      )
      sourceWsCtx <- getV2WorkspaceContextAndPermissions(entityCopyDef.sourceWorkspace,
                                                         SamWorkspaceActions.read,
                                                         Some(WorkspaceAttributeSpecs(all = false))
      )
      sourceAD <- samDAO.getResourceAuthDomain(SamResourceTypeNames.workspace, sourceWsCtx.workspaceId, ctx)
      destAD <- samDAO.getResourceAuthDomain(SamResourceTypeNames.workspace, destWsCtx.workspaceId, ctx)
      _ = authDomainCheck(sourceAD.toSet, destAD.toSet)
      entityRequestArguments = EntityRequestArguments(destWsCtx, ctx, None, None)
      entityProvider <- entityManager.resolveProviderFuture(entityRequestArguments)
      entityCopyResponse <- entityProvider
        .copyEntities(sourceWsCtx,
                      destWsCtx,
                      entityCopyDef.entityType,
                      entityCopyDef.entityNames,
                      linkExistingEntities,
                      ctx
        )
        .recover(bigQueryRecover)
    } yield entityCopyResponse)
      .recover(
        sqlLoggingRecover(s"copyEntities: $entityCopyDef $linkExistingEntities")
      )

  def batchUpdateEntitiesInternal(workspaceName: WorkspaceName,
                                  entityUpdates: Seq[EntityUpdateDefinition],
                                  upsert: Boolean,
                                  dataReference: Option[DataReferenceName],
                                  billingProject: Option[GoogleProjectId]
  ): Future[Traversable[Entity]] =
    getV2WorkspaceContextAndPermissions(workspaceName,
                                        SamWorkspaceActions.write,
                                        Some(WorkspaceAttributeSpecs(all = false))
    ) flatMap { workspaceContext =>
      val entityRequestArguments = EntityRequestArguments(workspaceContext, ctx, dataReference, billingProject)
      for {
        entityProvider <- entityManager.resolveProviderFuture(entityRequestArguments)
        entities <-
          if (upsert) {
            entityProvider.batchUpsertEntities(entityUpdates)
          } else {
            entityProvider.batchUpdateEntities(entityUpdates)
          }
      } yield entities
    }

  def batchUpdateEntities(workspaceName: WorkspaceName,
                          entityUpdates: Seq[EntityUpdateDefinition],
                          dataReference: Option[DataReferenceName],
                          billingProject: Option[GoogleProjectId]
  ): Future[Traversable[Entity]] =
    batchUpdateEntitiesInternal(workspaceName, entityUpdates, upsert = false, dataReference, billingProject)
      .recover(
        sqlLoggingRecover(s"batchUpdateEntities: $workspaceName ${entityUpdates.size} updates")
      )

  def batchUpsertEntities(workspaceName: WorkspaceName,
                          entityUpdates: Seq[EntityUpdateDefinition],
                          dataReference: Option[DataReferenceName],
                          billingProject: Option[GoogleProjectId]
  ): Future[Traversable[Entity]] =
    batchUpdateEntitiesInternal(workspaceName, entityUpdates, upsert = true, dataReference, billingProject)
      .recover(
        sqlLoggingRecover(s"batchUpsertEntities: $workspaceName ${entityUpdates.size} upserts")
      )

  def renameAttribute(workspaceName: WorkspaceName,
                      entityType: String,
                      oldAttributeName: AttributeName,
                      attributeRenameRequest: AttributeRename
  ): Future[Int] =
    withAttributeNamespaceCheck(Seq(attributeRenameRequest.newAttributeName)) {
      for {
        workspaceContext <- getV2WorkspaceContextAndPermissions(workspaceName,
                                                                SamWorkspaceActions.write,
                                                                Some(WorkspaceAttributeSpecs(all = false))
        )
        entityProvider <- entityManager.resolveProviderFuture(EntityRequestArguments(workspaceContext, ctx))
        result <- entityProvider.renameAttribute(entityType, oldAttributeName, attributeRenameRequest)
      } yield result
    }.recover(
      sqlLoggingRecover(s"renameAttribute: $workspaceName $oldAttributeName $attributeRenameRequest")
    )

  private def sqlLoggingRecover[U](logHint: String): PartialFunction[Throwable, U] = {
    case sqlException: SQLException =>
      // don't log a stack trace, and don't log at ERROR level;
      // these exceptions and their stack traces are already logged elsewhere. We just want to add logging
      // so we understand which method generated the exception.
      logger.warn(
        s"SQLException in EntityService ($logHint): ${sqlException.getClass.getName} - ${sqlException.getMessage}"
      )
      // rethrow as-is; RawlsApiService.exceptionHandler has handling we don't want to break
      throw sqlException;
  }

  private def bigQueryRecover[U]: PartialFunction[Throwable, U] = {
    case dee: DataEntityException =>
      throw new RawlsExceptionWithErrorReport(ErrorReport(dee.code, dee.getMessage))
    case bqe: BigQueryException =>
      throw new RawlsExceptionWithErrorReport(
        ErrorReport(StatusCodes.getForKey(bqe.getCode).getOrElse(StatusCodes.InternalServerError), bqe.getMessage)
      )
    case gjre: GoogleJsonResponseException =>
      // unlikely to hit this case; we should see BigQueryExceptions instead of GoogleJsonResponseExceptions
      throw new RawlsExceptionWithErrorReport(
        ErrorReport(StatusCodes.getForKey(gjre.getStatusCode).getOrElse(StatusCodes.InternalServerError),
                    gjre.getMessage
        )
      )
    case report: RawlsExceptionWithErrorReport =>
      throw report // don't rewrap these, just rethrow
    case ex: Exception =>
      throw new RawlsExceptionWithErrorReport(
        ErrorReport(StatusCodes.InternalServerError, s"Unexpected error: ${ex.getMessage}", ex)
      )
  }

}
