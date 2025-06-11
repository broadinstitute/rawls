package org.broadinstitute.dsde.rawls.entities

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.LazyLogging
import io.opentelemetry.api.common.AttributeKey
import org.apache.commons.lang3.time.StopWatch
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadAction, ReadWriteAction}
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.entities.base.{AuditLoggingEntityProvider, EntityProvider}
import org.broadinstitute.dsde.rawls.entities.exceptions.{
  DataEntityException,
  DeleteEntitiesConflictException,
  DeleteEntitiesOfTypeConflictException,
  EntityNotFoundException
}
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AttributeUpdateOperation, EntityUpdateDefinition}
import org.broadinstitute.dsde.rawls.model.WorkspaceSettingConfig.CompactDataTablesConfig
import org.broadinstitute.dsde.rawls.model.WorkspaceSettingTypes.CompactDataTables
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.TracingUtils.{
  setTraceSpanAttribute,
  traceDBIOWithParent,
  traceFutureWithParent
}
import org.broadinstitute.dsde.rawls.util.{AttributeSupport, EntitySupport, JsonFilterUtils, WorkspaceSupport}
import org.broadinstitute.dsde.rawls.workspace.{WorkspaceRepository, WorkspaceSettingService}
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, StringValidationUtils}
import slick.dbio.{DBIO, DBIOAction, Effect, NoStream}

import java.sql.SQLException
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

object EntityService {
  def constructor(dataSource: SlickDataSource,
                  samDAO: SamDAO,
                  workbenchMetricBaseName: String,
                  entityManager: EntityManager,
                  pageSizeLimit: Int,
                  workspaceSettingServiceConstructor: Option[RawlsRequestContext => WorkspaceSettingService] =
                    None // only used for Quicksilver migration
  )(ctx: RawlsRequestContext)(implicit executionContext: ExecutionContext, system: ActorSystem): EntityService =
    new EntityService(ctx,
                      dataSource,
                      samDAO,
                      entityManager,
                      workbenchMetricBaseName,
                      pageSizeLimit,
                      workspaceSettingServiceConstructor
    )
}

class EntityService(protected val ctx: RawlsRequestContext,
                    val dataSource: SlickDataSource,
                    val samDAO: SamDAO,
                    entityManager: EntityManager,
                    override val workbenchMetricBaseName: String,
                    pageSizeLimit: Int,
                    workspaceSettingServiceConstructor: Option[RawlsRequestContext => WorkspaceSettingService] =
                      None // only used for Quicksilver migration
)(implicit protected val executionContext: ExecutionContext, system: ActorSystem)
    extends WorkspaceSupport
    with EntitySupport
    with AttributeSupport
    with LazyLogging
    with RawlsInstrumented
    with JsonFilterUtils
    with StringValidationUtils {

  implicit override val errorReportSource: ErrorReportSource = ErrorReportSource("rawls")

  // used by WorkspaceSupport - in future refactoring, this can be moved into the constructor for better mocking
  val workspaceRepository: WorkspaceRepository = new WorkspaceRepository(dataSource)

  def createEntity(workspaceName: WorkspaceName, entity: Entity): Future[Entity] =
    traceFutureWithParent("EntityService.createEntity", ctx) { localContext =>
      withAttributeNamespaceCheck(entity) {
        for {
          workspaceContext <- traceFutureWithParent("getV2WorkspaceContextAndPermissions", localContext) { _ =>
            getV2WorkspaceContextAndPermissions(workspaceName,
                                                SamWorkspaceActions.write,
                                                Some(WorkspaceAttributeSpecs(all = false))
            )
          }
          entityProvider <- getProviderWithTracing(workspaceContext, localContext)
          result <- traceFutureWithParent("EntityProvider.createEntity", localContext) { s =>
            entityProvider.createEntity(entity, s)
          }
        } yield result
      }.recover(sqlLoggingRecover(s"createEntity: $workspaceName"))
    }

  def getEntity(workspaceName: WorkspaceName, entityType: String, entityName: String): Future[Entity] =
    traceFutureWithParent("EntityService.getEntity", ctx) { localContext =>
      traceFutureWithParent("getV2WorkspaceContextAndPermissions", localContext) { _ =>
        getV2WorkspaceContextAndPermissions(workspaceName,
                                            SamWorkspaceActions.read,
                                            Some(WorkspaceAttributeSpecs(all = false))
        )
      } flatMap { workspaceContext =>
        val entityFuture = for {
          entityProvider <- getProviderWithTracing(workspaceContext, localContext)
          entity <- traceFutureWithParent("EntityProvider.getEntity", localContext) { s =>
            entityProvider.getEntity(entityType, entityName, s)
          }
        } yield entity

        entityFuture
          .recover { case _: EntityNotFoundException =>
            // could move this error message into EntityNotFoundException and allow it to bubble up
            throw new RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.NotFound, s"$entityType $entityName does not exist in $workspaceName")
            )
          }
          .recover(sqlLoggingRecover(s"getEntity: $workspaceName $entityType/$entityName"))
          .recover(queryRecover)
      }
    }

  def updateEntity(workspaceName: WorkspaceName,
                   entityType: String,
                   entityName: String,
                   operations: Seq[AttributeUpdateOperation]
  ): Future[Entity] =
    traceFutureWithParent("EntityService.updateEntity", ctx) { localContext =>
      withAttributeNamespaceCheck(operations.map(_.name)) {
        for {
          workspaceContext <- traceFutureWithParent("getV2WorkspaceContextAndPermissions", localContext) { _ =>
            getV2WorkspaceContextAndPermissions(workspaceName,
                                                SamWorkspaceActions.write,
                                                Some(WorkspaceAttributeSpecs(all = false))
            )
          }
          entityProvider <- getProviderWithTracing(workspaceContext, localContext)
          result <- traceFutureWithParent("EntityProvider.updateEntity", localContext) { s =>
            entityProvider.updateEntity(entityType, entityName, operations, s)
          }
        } yield result
      }.recover(
        sqlLoggingRecover(s"updateEntity: $workspaceName $entityType/$entityName ${operations.size} operations")
      )
    }

  def deleteEntities(workspaceName: WorkspaceName,
                     entRefs: Seq[AttributeEntityReference]
  ): Future[Set[AttributeEntityReference]] =
    traceFutureWithParent("EntityService.deleteEntities", ctx) { localContext =>
      // short-circuit: if caller requested to delete nothing, then we do nothing
      if (entRefs.isEmpty) {
        Future.successful(Set.empty)
      } else {
        traceFutureWithParent("getV2WorkspaceContextAndPermissions", localContext) { _ =>
          getV2WorkspaceContextAndPermissions(workspaceName,
                                              SamWorkspaceActions.write,
                                              Some(WorkspaceAttributeSpecs(all = false))
          )
        } flatMap { workspaceContext =>
          val deleteFuture = for {
            entityProvider <- getProviderWithTracing(workspaceContext, localContext)
            _ <- traceFutureWithParent("entityProvider.deleteEntities", localContext) { s =>
              entityProvider.deleteEntities(entRefs.map(_.toPointer), s)
            }
          } yield Set[AttributeEntityReference]()

          deleteFuture
            .recover { case delEx: DeleteEntitiesConflictException =>
              delEx.referringEntities
            }
            .recover(sqlLoggingRecover(s"deleteEntities: $workspaceName ${entRefs.size} entities"))
            .recover(queryRecover)
        }
      }
    }

  def deleteEntitiesOfType(workspaceName: WorkspaceName,
                           entityType: String,
                           dataReference: Option[DataReferenceName],
                           billingProject: Option[GoogleProjectId]
  ): Future[Int] =
    traceFutureWithParent("EntityService.deleteEntitiesOfType", ctx) { localContext =>
      traceFutureWithParent("getV2WorkspaceContextAndPermissions", localContext) { _ =>
        getV2WorkspaceContextAndPermissions(workspaceName,
                                            SamWorkspaceActions.write,
                                            Some(WorkspaceAttributeSpecs(all = false))
        )
      } flatMap { workspaceContext =>
        val deleteFuture = for {
          entityProvider <- getProviderWithTracing(workspaceContext, localContext)
          numberOfEntitiesDeleted <- traceFutureWithParent("EntityProvider.deleteEntitiesOfType", localContext) { s =>
            entityProvider.deleteEntitiesOfType(entityType, s)
          }
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
          .recover(queryRecover)
      }
    }

  def deleteEntityAttributes(workspaceName: WorkspaceName,
                             entityType: String,
                             attributeNames: Set[AttributeName]
  ): Future[Unit] =
    traceFutureWithParent("EntityService.deleteEntityAttributes", ctx) { localContext =>
      (for {
        workspaceContext <- traceFutureWithParent("getV2WorkspaceContextAndPermissions", localContext) { _ =>
          getV2WorkspaceContextAndPermissions(workspaceName,
                                              SamWorkspaceActions.write,
                                              Some(WorkspaceAttributeSpecs(all = false))
          )
        }
        entityProvider <- getProviderWithTracing(workspaceContext, localContext)
        result <- traceFutureWithParent("EntityProvider.deleteEntityAttributes", localContext) { s =>
          entityProvider.deleteEntityAttributes(entityType, attributeNames, s)
        }
      } yield result)
        .recover(
          sqlLoggingRecover(
            s"deleteEntityAttributes: $workspaceName $entityType ${attributeNames.size} attribute names"
          )
        )
    }

  def renameEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, newName: String): Future[Int] =
    traceFutureWithParent("EntityService.renameEntity", ctx) { localContext =>
      (for {
        workspaceContext <- traceFutureWithParent("getV2WorkspaceContextAndPermissions", localContext) { _ =>
          getV2WorkspaceContextAndPermissions(workspaceName,
                                              SamWorkspaceActions.write,
                                              Some(WorkspaceAttributeSpecs(all = false))
          )
        }
        entityProvider <- getProviderWithTracing(workspaceContext, localContext)
        result <- traceFutureWithParent("EntityProvider.renameEntity", localContext) { s =>
          entityProvider.renameEntity(entityType, entityName, newName, s)
        }
      } yield result).recover(
        sqlLoggingRecover(s"renameEntity: $workspaceName $entityType $entityName")
      )
    }

  def renameEntityType(workspaceName: WorkspaceName, oldName: String, renameInfo: EntityTypeRename): Future[Int] =
    traceFutureWithParent("EntityService.renameEntityType", ctx) { localContext =>
      validateEntityType(renameInfo.newName)
      (for {
        workspaceContext <- traceFutureWithParent("getV2WorkspaceContextAndPermissions", localContext) { _ =>
          getV2WorkspaceContextAndPermissions(workspaceName,
                                              SamWorkspaceActions.write,
                                              Some(WorkspaceAttributeSpecs(all = false))
          )
        }
        entityProvider <- getProviderWithTracing(workspaceContext, localContext)
        result <- traceFutureWithParent("EntityProvider.renameEntityType", localContext) { s =>
          entityProvider.renameEntityType(oldName, renameInfo, s)
        }
      } yield result).recover(
        sqlLoggingRecover(s"renameEntityType: $workspaceName $workspaceName $oldName")
      )
    }

  def evaluateExpression(workspaceName: WorkspaceName,
                         entityType: String,
                         entityName: String,
                         expression: String
  ): Future[Seq[AttributeValue]] =
    traceFutureWithParent("EntityService.evaluateExpression", ctx) { localContext =>
      (for {
        workspaceContext <- traceFutureWithParent("getV2WorkspaceContextAndPermissions", localContext) { _ =>
          getV2WorkspaceContextAndPermissions(workspaceName,
                                              SamWorkspaceActions.read,
                                              Some(WorkspaceAttributeSpecs(all = false))
          )
        }
        entityProvider <- getProviderWithTracing(workspaceContext, localContext)
        result <- traceFutureWithParent("EntityProvider.evaluateExpression", localContext) { s =>
          entityProvider.evaluateExpression(entityType, entityName, expression, s)
        }
      } yield result).recover(
        sqlLoggingRecover(s"evaluateExpression: $workspaceName $entityType $entityName $expression")
      )
    }

  def entityTypeMetadata(workspaceName: WorkspaceName, useCache: Boolean): Future[Map[String, EntityTypeMetadata]] =
    traceFutureWithParent("EntityService.entityTypeMetadata", ctx) { localContext =>
      (traceFutureWithParent("getV2WorkspaceContextAndPermissions", localContext) { _ =>
        getV2WorkspaceContextAndPermissions(workspaceName,
                                            SamWorkspaceActions.read,
                                            Some(WorkspaceAttributeSpecs(all = false))
        )
      } flatMap { workspaceContext =>
        val metadataFuture = for {
          entityProvider <- getProviderWithTracing(workspaceContext, localContext)
          metadata <- traceFutureWithParent("EntityProvider.entityTypeMetadata", localContext) { s =>
            entityProvider.entityTypeMetadata(useCache, s)
          }
        } yield metadata

        metadataFuture.recover(queryRecover)
      }).recover(
        sqlLoggingRecover(s"entityTypeMetadata: $workspaceName")
      )
    }

  def listEntities(workspaceName: WorkspaceName, entityType: String): Future[Source[Entity, NotUsed]] =
    traceFutureWithParent("EntityService.listEntities", ctx) { localContext =>
      (for {
        workspaceContext <- traceFutureWithParent("getV2WorkspaceContextAndPermissions", localContext) { _ =>
          getV2WorkspaceContextAndPermissions(workspaceName,
                                              SamWorkspaceActions.read,
                                              Some(WorkspaceAttributeSpecs(all = false))
          )
        }
        entityProvider <- getProviderWithTracing(workspaceContext, localContext)
        result = entityProvider.listEntities(entityType)
      } yield result).recover(
        sqlLoggingRecover(s"listEntities: $workspaceName $entityType")
      )
    }

  def queryEntitiesSource(workspaceName: WorkspaceName,
                          entityType: String,
                          query: EntityQuery
  ): Future[(EntityQueryResultMetadata, Source[Entity, _])] =
    traceFutureWithParent("EntityService.queryEntitiesSource", ctx) { localContext =>
      if (query.pageSize > pageSizeLimit) {
        throw new RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.BadRequest, s"Page size cannot exceed $pageSizeLimit")
        )
      }

      traceFutureWithParent("getV2WorkspaceContextAndPermissions", localContext) { _ =>
        getV2WorkspaceContextAndPermissions(workspaceName,
                                            SamWorkspaceActions.read,
                                            Some(WorkspaceAttributeSpecs(all = false))
        )
      } flatMap { workspaceContext =>
        val queryFuture = for {
          entityProvider <- getProviderWithTracing(workspaceContext, localContext)
          metadataAndEntitySource <- traceFutureWithParent("EntityProvider.queryEntitiesSource", localContext) { s =>
            entityProvider.queryEntitiesSource(entityType, query, s)
          }
        } yield metadataAndEntitySource

        queryFuture.recover(queryRecover)
      }
    }

  def copyEntities(entityCopyDef: EntityCopyDefinition, linkExistingEntities: Boolean): Future[EntityCopyResponse] =
    traceFutureWithParent("EntityService.copyEntities", ctx) { localContext =>
      (for {
        destWsCtx <- traceFutureWithParent("getV2WorkspaceContextAndPermissions destWs", localContext) { _ =>
          getV2WorkspaceContextAndPermissions(entityCopyDef.destinationWorkspace,
                                              SamWorkspaceActions.write,
                                              Some(WorkspaceAttributeSpecs(all = false))
          )
        }
        sourceWsCtx <- traceFutureWithParent("getV2WorkspaceContextAndPermissions sourceWs", localContext) { _ =>
          getV2WorkspaceContextAndPermissions(entityCopyDef.sourceWorkspace,
                                              SamWorkspaceActions.read,
                                              Some(WorkspaceAttributeSpecs(all = false))
          )
        }
        sourceAD <- traceFutureWithParent("getSourceAuthDomain", localContext) { s =>
          samDAO.getResourceAuthDomain(SamResourceTypeNames.workspace, sourceWsCtx.workspaceId, s)
        }
        destAD <- traceFutureWithParent("getDestAuthDomain", localContext) { s =>
          samDAO.getResourceAuthDomain(SamResourceTypeNames.workspace, destWsCtx.workspaceId, s)
        }
        _ = authDomainCheck(sourceAD.toSet, destAD.toSet)
        entityProvider <- getProviderWithTracing(destWsCtx, localContext)

        sourceCompactEnabled <- isCompactDataTableSettingEnabled(entityCopyDef.sourceWorkspace)
        destCompactEnabled <- isCompactDataTableSettingEnabled(entityCopyDef.destinationWorkspace)
        _ = if (sourceCompactEnabled != destCompactEnabled) {
          throw new RawlsExceptionWithErrorReport(
            ErrorReport(
              StatusCodes.BadRequest,
              "Only one workspace has the CompactDataTablesSetting enabled. This setting must match on the source and destination workspace in order to copy entities."
            )
          )
        }

        entityCopyResponse <- traceFutureWithParent("EntityManager.resolveProviderFuture", localContext) { s =>
          entityProvider
            .copyEntities(sourceWsCtx,
                          destWsCtx,
                          entityCopyDef.entityType,
                          entityCopyDef.entityNames,
                          linkExistingEntities,
                          s
            )
            .recover(queryRecover)
        }
      } yield entityCopyResponse)
        .recover(
          sqlLoggingRecover(s"copyEntities: $entityCopyDef $linkExistingEntities")
        )
    }

  def batchUpdateEntitiesInternal(workspaceName: WorkspaceName,
                                  entityUpdates: Source[EntityUpdateDefinition, _],
                                  upsert: Boolean,
                                  parentContext: RawlsRequestContext
  ): Future[Int] =
    traceFutureWithParent("getV2WorkspaceContextAndPermissions", parentContext) { _ =>
      getV2WorkspaceContextAndPermissions(workspaceName,
                                          SamWorkspaceActions.write,
                                          Some(WorkspaceAttributeSpecs(all = false))
      )
    } flatMap { workspaceContext =>
      for {
        entityProvider <- getProviderWithTracing(workspaceContext, parentContext)
        entities <-
          if (upsert) {
            traceFutureWithParent("EntityProvider.batchUpsertEntities", parentContext) { s =>
              entityProvider.batchUpsertEntities(entityUpdates, s)
            }
          } else {
            traceFutureWithParent("EntityProvider.batchUpdateEntities", parentContext) { s =>
              entityProvider.batchUpdateEntities(entityUpdates, s)
            }
          }
      } yield entities
    }

  def batchUpdateEntities(workspaceName: WorkspaceName, entityUpdates: Source[EntityUpdateDefinition, _]): Future[Int] =
    traceFutureWithParent("EntityService.batchUpdateEntities", ctx) { s =>
      batchUpdateEntitiesInternal(workspaceName, entityUpdates, upsert = false, s)
        .recover(
          sqlLoggingRecover(s"batchUpdateEntities: $workspaceName")
        )
    }

  def batchUpsertEntities(workspaceName: WorkspaceName, entityUpdates: Source[EntityUpdateDefinition, _]): Future[Int] =
    traceFutureWithParent("EntityService.batchUpsertEntities", ctx) { s =>
      batchUpdateEntitiesInternal(workspaceName, entityUpdates, upsert = true, s)
        .recover(
          sqlLoggingRecover(s"batchUpsertEntities: $workspaceName")
        )
    }

  def saveWorkflowOutputEntities(
    dataAccess: DataAccess,
    workspace: Workspace,
    updatedEntities: Seq[Entity]
  ): ReadWriteAction[Traversable[Entity]] =
    for {
      provider <- DBIO.from(getProviderWithTracing(workspace, ctx))
      res <- provider.saveWorkflowOutputEntities(dataAccess, workspace, updatedEntities)
    } yield res

  def listWorkflowEntities(dataAccess: DataAccess,
                           workspace: Workspace,
                           entityIds: Seq[Long]
  ): ReadAction[Map[Long, Entity]] =
    for {
      provider <- DBIO.from(getProviderWithTracing(workspace, ctx))
      res <- provider.listWorkflowEntities(dataAccess, workspace, entityIds)
    } yield res

  def renameAttribute(workspaceName: WorkspaceName,
                      entityType: String,
                      oldAttributeName: AttributeName,
                      attributeRenameRequest: AttributeRename
  ): Future[Int] = traceFutureWithParent("EntityService.renameAttribute", ctx) { localContext =>
    withAttributeNamespaceCheck(Seq(attributeRenameRequest.newAttributeName)) {
      for {
        workspaceContext <- traceFutureWithParent("getV2WorkspaceContextAndPermissions", localContext) { _ =>
          getV2WorkspaceContextAndPermissions(workspaceName,
                                              SamWorkspaceActions.write,
                                              Some(WorkspaceAttributeSpecs(all = false))
          )
        }
        entityProvider <- getProviderWithTracing(workspaceContext, localContext)
        result <- traceFutureWithParent("EntityProvider.renameAttribute", localContext) { s =>
          entityProvider.renameAttribute(entityType, oldAttributeName, attributeRenameRequest, s)
        }
      } yield result
    }.recover(
      sqlLoggingRecover(s"renameAttribute: $workspaceName $oldAttributeName $attributeRenameRequest")
    )
  }

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

  private def queryRecover[U]: PartialFunction[Throwable, U] = {
    case dee: DataEntityException =>
      throw new RawlsExceptionWithErrorReport(ErrorReport(dee.code, dee.getMessage))
    case report: RawlsExceptionWithErrorReport =>
      throw report // don't rewrap these, just rethrow
    case ex: Exception =>
      throw new RawlsExceptionWithErrorReport(
        ErrorReport(StatusCodes.InternalServerError, s"Unexpected error: ${ex.getMessage}", ex)
      )
  }

  /**
   * Helper to get the appropriate EntityProvider for the workspace while also adding tracing info
   */
  private def getProviderWithTracing(workspaceContext: Workspace,
                                     localContext: RawlsRequestContext
  ): Future[EntityProvider] =
    for {
      entityProvider <- traceFutureWithParent("EntityManager.resolveProviderFuture", localContext) { s =>
        entityManager.resolveProviderFuture(EntityRequestArguments(workspaceContext, s))
      }
      providerName = entityProvider match {
        case audit: AuditLoggingEntityProvider =>
          s"${audit.delegate.getClass.getSimpleName} via AuditLoggingEntityProvider"
        case x => x.getClass.getSimpleName
      }
      _ = setTraceSpanAttribute(localContext, AttributeKey.stringKey("providerType"), providerName)
    } yield entityProvider

  /**
   * Determine if a workspace has the CompactDataTables setting enabled.
   */
  private def isCompactDataTableSettingEnabled(workspaceName: WorkspaceName): Future[Boolean] =
    workspaceSettingServiceConstructor match {
      case Some(serviceConstructor) =>
        val workspaceSettingService = serviceConstructor(ctx)
        workspaceSettingService.getWorkspaceSettingOfType(workspaceName, CompactDataTables) map {
          case Some(qs: CompactDataTablesSetting) => qs.config.enabled
          case _                                  => false
        }
      case None =>
        throw new RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.InternalServerError, "Workspace setting service not available")
        )
    }

  /**
    * Migrate all entity data in a given workspace from legacy (LocalEntityProvider) to
    * compact (Quicksilver) format.
    * The migration relies on temp tables; the batchSize setting ensures the temp tables do not grow too large.
    *
    * @param workspaceName the name of the workspace to migrate
    * @param batchSize the number of entities to migrate in a single batch; defaults to 50,000
    */
  def quicksilverMigration(workspaceName: WorkspaceName, batchSize: Int = 50000): Future[Int] =
    traceFutureWithParent("EntityService.quicksilverMigration", ctx) { s =>
      for {
        // verify owner of workspace.
        workspaceContext <- traceFutureWithParent("getV2WorkspaceContextAndPermissions", s) { _ =>
          getV2WorkspaceContextAndPermissions(workspaceName,
                                              SamWorkspaceActions.own,
                                              Some(WorkspaceAttributeSpecs(all = false))
          )
        }
        workspaceId = workspaceContext.workspaceIdAsUUID

        // confirm if this is already a quicksilver workspace by checking settings
        workspaceSettingService = workspaceSettingServiceConstructor.get.apply(ctx)
        settings <- traceFutureWithParent("getWorkspaceSettings", s) { _ =>
          workspaceSettingService.getWorkspaceSettings(workspaceName)
        }
        _ = if (
          settings
            .find(_.isInstanceOf[CompactDataTablesSetting])
            .asInstanceOf[Option[CompactDataTablesSetting]]
            .exists(_.config.enabled)
        ) {
          throw new RawlsExceptionWithErrorReport(ErrorReport("Quicksilver already enabled for this workspace"))
        }

        // start a transaction; here's where we do a bunch of writes
        userResult <- dataSource.inTransaction { dataAccess =>
          val shardId: String = dataAccess.determineShard(workspaceId)

          val stopwatch = StopWatch.createStarted()

          logger.info(
            s"Quicksilver migration $workspaceId: starting (${stopwatch.formatTime()}) ..."
          )

          withIncreasedSortMemory(dataAccess) {
            for {
              // batch into groups of $batchSize entities, currently 50k. This method returns the min and max entity ids
              // for each batch. later queries will use those boundaries to migrate entities in batches, which
              // prevents the temp tables from growing too large.
              batchBoundaries <- quicksilverCalculateBatches(workspaceId, batchSize, dataAccess)
              // niceties for logging
              indexedBoundaries = batchBoundaries.zipWithIndex

              // for each batch, migrate the entities in that batch
              updateCounts <- DBIO.sequence(indexedBoundaries.map { case (boundary, idx) =>
                quicksilverMigrateBatch(workspaceId,
                                        shardId,
                                        boundary,
                                        dataAccess,
                                        idx,
                                        indexedBoundaries.size,
                                        stopwatch,
                                        s
                )
              })
              numEntitiesUpdated = updateCounts.sum

              // populate the ENTITY_REFS table for this workspace
              _ <- traceDBIOWithParent("migrationAddReferences", s) { _ =>
                dataAccess.compactEntityQuery.migrationAddReferences(workspaceId, shardId)
              }
              _ = logger.info(
                s"Quicksilver migration $workspaceId: populated ENTITY_REFS (${stopwatch.formatTime()}) ..."
              )

              /** *** don't delete legacy data; we'll do that en masse after everything is migrated
              *  // delete legacy attributes from the ENTITY_ATTRIBUTE_xx_xx table
              *  _ = logger.info(s"Quicksilver migration: deleting legacy attributes ...")
              *  _ <- dataAccess.compactEntityQuery.migrationDeleteLegacyReferences(workspaceContext.workspaceIdAsUUID,
              *  shardId
              *  )
              *  // delete the all_attribute_values column for this workspace
              *  _ = logger.info(s"Quicksilver migration: clearing all_attribute_values ...")
              *  _ <- dataAccess.compactEntityQuery.migrationClearAllAttributesString(workspaceContext.workspaceIdAsUUID)
              * *** */

              _ = logger.info(
                s"Quicksilver migration $workspaceId: done! $numEntitiesUpdated entities updated. (${stopwatch.formatTime()})"
              )
            } yield numEntitiesUpdated
          }

        }

        // finally, change the workspace to be quicksilver-enabled
        _ <- traceFutureWithParent("setWorkspaceSettings", s) { _ =>
          workspaceSettingService.setWorkspaceSettings(
            workspaceName,
            List(CompactDataTablesSetting(CompactDataTablesConfig(enabled = true)))
          )
        }

        // return a count of entities updated
      } yield userResult
    }

  /** Executes a database operation `op` in a session using 8MB of `sort_buffer_size` memory,
    * then resets the sort buffer size back to its original value */
  private def withIncreasedSortMemory[T](dataAccess: DataAccess)(op: => ReadWriteAction[T]): ReadWriteAction[T] =
    for {
      // get the current value of MySQL sort_buffer_size
      defaultSortBufferSize <- dataAccess.compactEntityQuery.getSortBufferSetting
      // set sort buffer size to 8MB; this avoids MySQL errors during migration
      // with an "Out of sort memory, consider increasing server sort buffer size" message.
      // see also https://bugs.mysql.com/bug.php?id=103225
      _ <- dataAccess.compactEntityQuery.setSessionSortBuffer(8388608L)
      // execute the requested operation, then reset sort buffer size to its original value
      result <- op andFinally dataAccess.compactEntityQuery.setSessionSortBuffer(defaultSortBufferSize)
    } yield result

  private case class MigrationBoundary(startEntityId: Long, endEntityId: Long)

  /**
   * Calculate the boundaries for each batch of entities to migrate
   * This method returns a sequence of MigrationBoundary objects, each containing the start and end entity ids
   * for a batch. The batch sizes are determined by the batchSize parameter.
   */
  private def quicksilverCalculateBatches(workspaceId: UUID,
                                          batchSize: Int,
                                          dataAccess: DataAccess
  ): ReadAction[Seq[MigrationBoundary]] = {

    def findNextBatch(accum: Seq[MigrationBoundary], startingId: Long): ReadAction[Seq[MigrationBoundary]] =
      dataAccess.compactEntityQuery.findMaxBatchId(batchSize, startingId, workspaceId).flatMap {
        case None | Some(0L) => DBIO.successful(accum)
        case Some(nextLimit) => findNextBatch(accum :+ MigrationBoundary(startingId, nextLimit), nextLimit)
      }

    findNextBatch(Seq.empty, -1)
  }

  /**
    * Perform the work to migrate a single batch of entities from legacy to compact format.
    * This method will:
    *   - create temporary tables for attributes and entities
    *   - populate the attribute temp table with JSON-normalized attributes
    *   - populate the entity temp table with a single JSON object per entity
    *   - update the ENTITY table from the entity temp table
    *   - drop the temporary tables
    */
  private def quicksilverMigrateBatch(workspaceId: UUID,
                                      shardId: String,
                                      boundary: MigrationBoundary,
                                      dataAccess: DataAccess,
                                      batchIdx: Int,
                                      totalBatches: Int,
                                      stopwatch: StopWatch,
                                      parentContext: RawlsRequestContext
  ): ReadWriteAction[Int] = {

    // instrumentation helper
    def logAndTrace[T, E <: Effect](spanName: String, logMessage: String)(
      op: DBIOAction[T, NoStream, E]
    ): DBIOAction[T, NoStream, E with Effect] =
      traceDBIOWithParent(spanName, parentContext) { _ =>
        op
      }.map { result =>
        logger.info(
          s"Quicksilver migration $workspaceId batch ${batchIdx + 1}/$totalBatches: $logMessage (${stopwatch.formatTime()}) ..."
        )
        result
      }

    (for {
      // create temp tables
      _ <- logAndTrace("migrationCreateTempTables", "created migration temp tables") {
        DBIO.seq(dataAccess.compactEntityQuery.migrationCreateAttributeTempTable,
                 dataAccess.compactEntityQuery.migrationCreateEntityTempTable
        )
      }
      // normalize attributes to JSON scalars and insert into the temp table
      _ <- logAndTrace("migrationPopulateAttributeTempTable", "populated attribute temp table") {
        dataAccess.compactEntityQuery.migrationPopulateAttributeTempTable(workspaceId,
                                                                          shardId,
                                                                          boundary.startEntityId,
                                                                          boundary.endEntityId
        )
      }
      // combine scalars into arrays; join all attributes into a single JSON object per entity; populate entity temp
      _ <- logAndTrace("migrationPopulateEntityTempTable", "populated entity temp table") {
        dataAccess.compactEntityQuery.migrationPopulateEntityTempTable
      }

      // update ENTITY from the contents of the temp table
      numEntitiesUpdated <- logAndTrace("migrationUpdateEntityTable", "updated ENTITY from temp table") {
        dataAccess.compactEntityQuery.migrationUpdateEntityTable(workspaceId)
      }

    } yield numEntitiesUpdated) andFinally
      // drop temp tables
      // this explicitly uses create/drop table instead of truncate to avoid implicit transaction commits:
      // https://dev.mysql.com/doc/refman/8.0/en/implicit-commit.html
      logAndTrace("migrationDropTempTables", "dropped temp tables") {
        DBIO.seq(dataAccess.compactEntityQuery.migrationDropAttributeTempTable,
                 dataAccess.compactEntityQuery.migrationDropEntityTempTable
        )
      }
  }

}
