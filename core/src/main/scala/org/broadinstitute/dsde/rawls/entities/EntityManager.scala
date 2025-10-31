package org.broadinstitute.dsde.rawls.entities

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.entities.base.{AuditLoggingEntityProvider, EntityProvider, EntityProviderBuilder}
import org.broadinstitute.dsde.rawls.entities.exceptions.DataEntityException
import org.broadinstitute.dsde.rawls.entities.local.{LocalEntityProvider, LocalEntityProviderBuilder}
import org.broadinstitute.dsde.rawls.entities.compact.{CompactEntityProvider, CompactEntityProviderBuilder}
import org.broadinstitute.dsde.rawls.model.WorkspaceSettingTypes.CompactDataTables
import org.broadinstitute.dsde.rawls.model.{CloudPlatform, CompactDataTablesSetting, ErrorReport, WorkspaceType}
import org.broadinstitute.dsde.rawls.workspace.WorkspaceSettingRepository

import java.time.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success}

/**
 * Here's the philosophy behind the important entity classes:
 *
 * EntityProvider:
 *    these do the nuts-and-bolts work of connecting to a datasource and manipulating the entities therein.
 *    EntityProvider authors should not have to worry too much about thread safety, multitenancy, concurrency, etc - so,
 *    we create a new EntityProvider instance for each request.
 *
 *    Subclasses are:
 *      - LocalEntityProvider: the default. Legacy Rawls/CloudSQL implementation.
 *      - CompactEntityProvider: "Quicksilver" data tables, using JSON features in CloudSQL
 *
 * EntityProviderBuilder:
 *    since we create many instances of EntityProvider, we want a factory pattern. These builders are responsible
 *    for making the various EntityProvider instances. Builders should be singletons, and can be instantiated
 *    once with config values or other arguments that the provider instances will need.
 *
 * EntityManager:
 *    another singleton, the EntityManager is instantiated with the set of ProviderBuilders that this application
 *    knows about. The manager is responsible for inspecting the inbound request, determining which builder should
 *    be used to satisfy the request, and using that builder to create and return a provider instance.
 *
 */
class EntityManager(providerBuilders: Set[EntityProviderBuilder[_ <: EntityProvider]],
                    workspaceSettingRepository: WorkspaceSettingRepository,
                    metricsPrefix: String
) {

  /**
    * Returns the appropriate EntityProvider to use for the current request.
    * @param requestArguments description of the request
    * @return the provider
    */
  def resolveProviderFuture(
    requestArguments: EntityRequestArguments
  )(implicit executionContext: ExecutionContext): Future[EntityProvider] = {

    if (!WorkspaceType.RawlsWorkspace.equals(requestArguments.workspace.workspaceType)) {
      throw new DataEntityException(
        s"This API is disabled for ${CloudPlatform.AZURE} workspaces. Contact support for alternatives."
      )
    }

    // If the workspace has the CompactDataTables setting enabled and no pending CompactDataTables Settings,
    // use CompactEntityProvider; else use LocalEntityProvider.
    val compactDataTables = workspaceSettingRepository
      .hasPendingSettings(
        requestArguments.workspace.workspaceIdAsUUID,
        CompactDataTables
      )
      .flatMap { hasPending =>
        if (hasPending) {
          Future.failed(
            new DataEntityException(
              s"CompactDataTable migration is in progress for workspace ${requestArguments.workspace.toWorkspaceName}. Access is temporarily disabled."
            )
          )
        } else {
          workspaceSettingRepository
            .getWorkspaceSettingOfType(
              requestArguments.workspace.workspaceIdAsUUID,
              CompactDataTables
            )
            .map {
              case Some(qs: CompactDataTablesSetting) if qs.config.enabled => true
              case _                                                       => false
            }
        }
      }
    val targetTagFuture = compactDataTables map {
      case true  => typeTag[CompactEntityProvider]
      case false => typeTag[CompactEntityProvider]
    }

    targetTagFuture map { targetTag =>
      providerBuilders.find(_.builds == targetTag) match {
        case None =>
          throw new DataEntityException(
            s"no entity provider available for ${requestArguments.workspace.toWorkspaceName}"
          )
        case Some(builder) =>
          builder.build(requestArguments) match {
            case Success(provider) =>
              // Wrap the provider with AuditLoggingEntityProvider
              new AuditLoggingEntityProvider(provider, requestArguments, metricsPrefix)
            case Failure(regrets: DataEntityException) =>
              throw new RawlsExceptionWithErrorReport(ErrorReport(regrets.code, regrets.getMessage))
            case Failure(ex: Throwable) =>
              throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.InternalServerError, ex.getMessage))
          }
      }
    }
  }
}

object EntityManager {
  def defaultEntityManager(dataSource: SlickDataSource,
                           workspaceSettingRepository: WorkspaceSettingRepository,
                           cacheEnabled: Boolean,
                           queryTimeout: Duration,
                           metricsPrefix: String
  )(implicit ec: ExecutionContext, system: ActorSystem): EntityManager = {
    // create the EntityManager along with its associated provider-builders. Since entities are only accessed
    // in the context of a workspace, this is safe/correct to do here. We also want to use the same dataSource
    // and execution context for the rawls entity provider that the entity service uses.
    val defaultEntityProviderBuilder =
      new LocalEntityProviderBuilder(dataSource,
                                     cacheEnabled,
                                     queryTimeout,
                                     metricsPrefix
      ) // implicit executionContext, system
    val compactEntityProviderBuilder = new CompactEntityProviderBuilder(dataSource, metricsPrefix)

    new EntityManager(
      Set(defaultEntityProviderBuilder, compactEntityProviderBuilder),
      workspaceSettingRepository,
      metricsPrefix
    )
  }
}
