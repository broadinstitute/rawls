package org.broadinstitute.dsde.rawls.entities

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.stream.scaladsl.Source
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.cloud.bigquery.BigQueryException
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.entities.exceptions.{
  DataEntityException,
  DeleteEntitiesConflictException,
  DeleteEntitiesOfTypeConflictException,
  EntityNotFoundException
}
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AttributeUpdateOperation, EntityUpdateDefinition}
import org.broadinstitute.dsde.rawls.model.WorkspaceSettingConfig.CompactDataTablesConfig
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.TracingUtils.traceFutureWithParent
import org.broadinstitute.dsde.rawls.util.{AttributeSupport, EntitySupport, JsonFilterUtils, WorkspaceSupport}
import org.broadinstitute.dsde.rawls.workspace.{WorkspaceRepository, WorkspaceSettingService}
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, StringValidationUtils}
import slick.dbio.DBIO

import java.sql.SQLException
import scala.concurrent.{ExecutionContext, Future}

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
                    pageSizeLimit: Int,
                    workspaceSettingServiceConstructor: Option[RawlsRequestContext => WorkspaceSettingService] =
                      None // only used for Quicksilver migration
)(implicit protected val executionContext: ExecutionContext)
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
          entityProvider <- traceFutureWithParent("EntityManager.resolveProviderFuture", localContext) { s =>
            entityManager.resolveProviderFuture(EntityRequestArguments(workspaceContext, s))
          }
          result <- traceFutureWithParent("EntityProvider.createEntity", localContext) { s =>
            entityProvider.createEntity(entity, s)
          }
        } yield result
      }.recover(sqlLoggingRecover(s"createEntity: $workspaceName"))
    }

  def getEntity(workspaceName: WorkspaceName,
                entityType: String,
                entityName: String,
                dataReference: Option[DataReferenceName],
                billingProject: Option[GoogleProjectId]
  ): Future[Entity] =
    traceFutureWithParent("EntityService.getEntity", ctx) { localContext =>
      traceFutureWithParent("getV2WorkspaceContextAndPermissions", localContext) { _ =>
        getV2WorkspaceContextAndPermissions(workspaceName,
                                            SamWorkspaceActions.read,
                                            Some(WorkspaceAttributeSpecs(all = false))
        )
      } flatMap { workspaceContext =>
        val entityFuture = for {
          entityProvider <- traceFutureWithParent("EntityManager.resolveProviderFuture", localContext) { s =>
            entityManager.resolveProviderFuture(
              EntityRequestArguments(workspaceContext, s, dataReference, billingProject)
            )
          }
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
          .recover(bigQueryRecover)
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
          entityProvider <- traceFutureWithParent("EntityManager.resolveProviderFuture", localContext) { s =>
            entityManager.resolveProviderFuture(EntityRequestArguments(workspaceContext, s))
          }
          result <- traceFutureWithParent("EntityProvider.updateEntity", localContext) { s =>
            entityProvider.updateEntity(entityType, entityName, operations, s)
          }
        } yield result
      }.recover(
        sqlLoggingRecover(s"updateEntity: $workspaceName $entityType/$entityName ${operations.size} operations")
      )
    }

  def deleteEntities(workspaceName: WorkspaceName,
                     entRefs: Seq[AttributeEntityReference],
                     dataReference: Option[DataReferenceName],
                     billingProject: Option[GoogleProjectId]
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
            entityProvider <- traceFutureWithParent("EntityManager.resolveProviderFuture", localContext) { s =>
              entityManager.resolveProviderFuture(
                EntityRequestArguments(workspaceContext, s, dataReference, billingProject)
              )
            }
            _ <- traceFutureWithParent("entityProvider.deleteEntities", localContext) { s =>
              entityProvider.deleteEntities(entRefs, s)
            }
          } yield Set[AttributeEntityReference]()

          deleteFuture
            .recover { case delEx: DeleteEntitiesConflictException =>
              delEx.referringEntities
            }
            .recover(sqlLoggingRecover(s"deleteEntities: $workspaceName ${entRefs.size} entities"))
            .recover(bigQueryRecover)
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
          entityProvider <- traceFutureWithParent("EntityManager.resolveProviderFuture", localContext) { s =>
            entityManager.resolveProviderFuture(
              EntityRequestArguments(workspaceContext, s, dataReference, billingProject)
            )
          }
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
          .recover(bigQueryRecover)
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
        entityProvider <- traceFutureWithParent("EntityManager.resolveProviderFuture", localContext) { s =>
          entityManager.resolveProviderFuture(EntityRequestArguments(workspaceContext, s))
        }
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
        entityProvider <- traceFutureWithParent("EntityManager.resolveProviderFuture", localContext) { s =>
          entityManager.resolveProviderFuture(EntityRequestArguments(workspaceContext, s))
        }
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
        entityProvider <- traceFutureWithParent("EntityManager.resolveProviderFuture", localContext) { s =>
          entityManager.resolveProviderFuture(EntityRequestArguments(workspaceContext, s))
        }
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
        entityProvider <- traceFutureWithParent("EntityManager.resolveProviderFuture", localContext) { s =>
          entityManager.resolveProviderFuture(EntityRequestArguments(workspaceContext, s))
        }
        result <- traceFutureWithParent("EntityProvider.evaluateExpression", localContext) { s =>
          entityProvider.evaluateExpression(entityType, entityName, expression, s)
        }
      } yield result).recover(
        sqlLoggingRecover(s"evaluateExpression: $workspaceName $entityType $entityName $expression")
      )
    }

  def entityTypeMetadata(workspaceName: WorkspaceName,
                         dataReference: Option[DataReferenceName],
                         billingProject: Option[GoogleProjectId],
                         useCache: Boolean
  ): Future[Map[String, EntityTypeMetadata]] =
    traceFutureWithParent("EntityService.entityTypeMetadata", ctx) { localContext =>
      (traceFutureWithParent("getV2WorkspaceContextAndPermissions", localContext) { _ =>
        getV2WorkspaceContextAndPermissions(workspaceName,
                                            SamWorkspaceActions.read,
                                            Some(WorkspaceAttributeSpecs(all = false))
        )
      } flatMap { workspaceContext =>
        val metadataFuture = for {
          entityProvider <- traceFutureWithParent("EntityManager.resolveProviderFuture", localContext) { s =>
            entityManager.resolveProviderFuture(
              EntityRequestArguments(workspaceContext, s, dataReference, billingProject)
            )
          }
          metadata <- traceFutureWithParent("EntityProvider.entityTypeMetadata", localContext) { s =>
            entityProvider.entityTypeMetadata(useCache, s)
          }
        } yield metadata

        metadataFuture.recover(bigQueryRecover)
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
        entityProvider <- traceFutureWithParent("EntityManager.resolveProviderFuture", localContext) { s =>
          entityManager.resolveProviderFuture(EntityRequestArguments(workspaceContext, s))
        }
        result = entityProvider.listEntities(entityType)
      } yield result).recover(
        sqlLoggingRecover(s"listEntities: $workspaceName $entityType")
      )
    }

  def queryEntitiesSource(workspaceName: WorkspaceName,
                          dataReference: Option[DataReferenceName],
                          entityType: String,
                          query: EntityQuery,
                          billingProject: Option[GoogleProjectId]
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
          entityProvider <- traceFutureWithParent("EntityManager.resolveProviderFuture", localContext) { s =>
            entityManager.resolveProviderFuture(
              EntityRequestArguments(workspaceContext, s, dataReference, billingProject)
            )
          }
          metadataAndEntitySource <- traceFutureWithParent("EntityProvider.queryEntitiesSource", localContext) { s =>
            entityProvider.queryEntitiesSource(entityType, query, s)
          }
        } yield metadataAndEntitySource

        queryFuture.recover(bigQueryRecover)
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
        entityProvider <- traceFutureWithParent("EntityManager.resolveProviderFuture", localContext) { s =>
          entityManager.resolveProviderFuture(EntityRequestArguments(destWsCtx, s, None, None))
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
            .recover(bigQueryRecover)
        }
      } yield entityCopyResponse)
        .recover(
          sqlLoggingRecover(s"copyEntities: $entityCopyDef $linkExistingEntities")
        )
    }

  def batchUpdateEntitiesInternal(workspaceName: WorkspaceName,
                                  entityUpdates: Seq[EntityUpdateDefinition],
                                  upsert: Boolean,
                                  dataReference: Option[DataReferenceName],
                                  billingProject: Option[GoogleProjectId],
                                  parentContext: RawlsRequestContext
  ): Future[Traversable[Entity]] =
    traceFutureWithParent("getV2WorkspaceContextAndPermissions", parentContext) { _ =>
      getV2WorkspaceContextAndPermissions(workspaceName,
                                          SamWorkspaceActions.write,
                                          Some(WorkspaceAttributeSpecs(all = false))
      )
    } flatMap { workspaceContext =>
      for {
        entityProvider <- traceFutureWithParent("EntityManager.resolveProviderFuture", parentContext) { s =>
          entityManager.resolveProviderFuture(
            EntityRequestArguments(workspaceContext, s, dataReference, billingProject)
          )
        }
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

  def batchUpdateEntities(workspaceName: WorkspaceName,
                          entityUpdates: Seq[EntityUpdateDefinition],
                          dataReference: Option[DataReferenceName],
                          billingProject: Option[GoogleProjectId]
  ): Future[Traversable[Entity]] =
    traceFutureWithParent("EntityService.batchUpdateEntities", ctx) { s =>
      batchUpdateEntitiesInternal(workspaceName, entityUpdates, upsert = false, dataReference, billingProject, s)
        .recover(
          sqlLoggingRecover(s"batchUpdateEntities: $workspaceName ${entityUpdates.size} updates")
        )
    }

  def batchUpsertEntities(workspaceName: WorkspaceName,
                          entityUpdates: Seq[EntityUpdateDefinition],
                          dataReference: Option[DataReferenceName],
                          billingProject: Option[GoogleProjectId]
  ): Future[Traversable[Entity]] =
    traceFutureWithParent("EntityService.batchUpsertEntities", ctx) { s =>
      batchUpdateEntitiesInternal(workspaceName, entityUpdates, upsert = true, dataReference, billingProject, s)
        .recover(
          sqlLoggingRecover(s"batchUpsertEntities: $workspaceName ${entityUpdates.size} upserts")
        )
    }

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
        entityProvider <- traceFutureWithParent("EntityManager.resolveProviderFuture", localContext) { s =>
          entityManager.resolveProviderFuture(EntityRequestArguments(workspaceContext, s))
        }
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

  /**
    * Migrate all entity data in a given workspace from legacy (LocalEntityProvider) to compact (Quicksilver) format.
    *
    * This migration method is unoptimized and in flux; use at your own risk
    *
    */
  def quicksilverMigration(workspaceName: WorkspaceName): Future[Map[String, Int]] = {
    implicit val system: ActorSystem = ActorSystem("quicksilverMigration")

    for {
      // verify owner of workspace.
      // TODO CORE-364: require some kind of admin permission via asFCAdmin or a resource type admin instead?
      workspaceContext <- getV2WorkspaceContextAndPermissions(workspaceName,
                                                              SamWorkspaceActions.own,
                                                              Some(WorkspaceAttributeSpecs(all = false))
      )

      // confirm if this is already a quicksilver workspace by checking settings
      workspaceSettingService = workspaceSettingServiceConstructor.get.apply(ctx)
      settings <- workspaceSettingService.getWorkspaceSettings(workspaceName)
      _ = if (
        settings.find(_.isInstanceOf[CompactDataTablesSetting]).asInstanceOf[CompactDataTablesSetting].config.enabled
      ) {
        throw new RawlsExceptionWithErrorReport(ErrorReport("Quicksilver already enabled for this workspace"))
      }

      // get the local (legacy) provider
      localProvider <- entityManager.resolveProviderFuture(EntityRequestArguments(workspaceContext, ctx))

      // change the workspace to be quicksilver-enabled
      _ <- workspaceSettingService.setWorkspaceSettings(
        workspaceName,
        List(CompactDataTablesSetting(CompactDataTablesConfig(enabled = true)))
      )

      // get the list of entity types in this workspace
      entityTypeMetadata <- localProvider.entityTypeMetadata(useCache = true, ctx)

      // start a transaction; here's where we do a bunch of writes
      _ <- dataSource.inTransaction { dataAccess =>
        val shardId: String = dataAccess.determineShard(workspaceContext.workspaceIdAsUUID)

        // loop over entity types
        val allTypesResult = entityTypeMetadata.map { case (entityType, metadata) =>
          DBIO.from(
            // retrieve all active entities of this type, as a streamable Source
            localProvider
              .listEntities(entityType)
              // stream over each entity and ...
              .map { entity =>
                // ... update the existing row in the ENTITY table to populate the attributes column
                dataAccess.compactEntityQuery.migrationUpdateAttributes(workspaceContext.workspaceIdAsUUID, entity)
              }
              .run()
          )
        }

        for {
          // migrate all attribute data from ENTITY_ATTRIBUTES_xx_xx to ENTITY.attributes
          _ <- DBIO.sequence(allTypesResult)
          // populate the ENTITY_REFS table for this workspace
          _ <- dataAccess.compactEntityQuery.migrationAddReferences(workspaceContext.workspaceIdAsUUID, shardId)
          // delete legacy attributes from the ENTITY_ATTRIBUTE_xx_xx table
          _ <- dataAccess.compactEntityQuery.migrationDeleteLegacyReferences(workspaceContext.workspaceIdAsUUID,
                                                                             shardId
          )
        } yield ()
      }

      // return a count of entities updated
    } yield entityTypeMetadata.map { case (entityType, metadata) => (entityType, metadata.count) }
  }
}
