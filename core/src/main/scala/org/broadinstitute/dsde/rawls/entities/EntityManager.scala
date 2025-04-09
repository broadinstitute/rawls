package org.broadinstitute.dsde.rawls.entities

import akka.http.scaladsl.model.StatusCodes
import bio.terra.workspace.model.CloudPlatform
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.config.DataRepoEntityProviderConfig
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.DataRepoDAO
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleBigQueryServiceFactory, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.entities.base.{EntityProvider, EntityProviderBuilder}
import org.broadinstitute.dsde.rawls.entities.datarepo.{DataRepoEntityProvider, DataRepoEntityProviderBuilder}
import org.broadinstitute.dsde.rawls.entities.exceptions.DataEntityException
import org.broadinstitute.dsde.rawls.entities.local.{LocalEntityProvider, LocalEntityProviderBuilder}
import org.broadinstitute.dsde.rawls.entities.compact.{CompactEntityProvider, CompactEntityProviderBuilder}
import org.broadinstitute.dsde.rawls.model.WorkspaceSettingTypes.CompactDataTables
import org.broadinstitute.dsde.rawls.model.{CompactDataTablesSetting, ErrorReport, WorkspaceType}
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
 *      - DataRepoEntityProvider: for working with Terra Data Repo snapshots.
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
                    workspaceSettingRepository: WorkspaceSettingRepository
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

    // soon: look up the reference name to ensure it exists.
    // for now, this simplistic logic illustrates the approach: choose the right builder for the job.
    val targetTagFuture = if (requestArguments.dataReference.isDefined) {
      Future.successful(typeTag[DataRepoEntityProvider])
    } else {
      val compactDataTables =
        workspaceSettingRepository.getWorkspaceSettingOfType(requestArguments.workspace.workspaceIdAsUUID,
                                                             CompactDataTables
        ) map {
          case Some(qs: CompactDataTablesSetting) => qs.config.enabled
          case _                                  => false
        }
      compactDataTables map {
        case true  => typeTag[CompactEntityProvider]
        case false => typeTag[LocalEntityProvider]
      }
    }

    targetTagFuture map { targetTag =>
      providerBuilders.find(_.builds == targetTag) match {
        case None =>
          throw new DataEntityException(
            s"no entity provider available for ${requestArguments.workspace.toWorkspaceName}"
          )
        case Some(builder) =>
          builder.build(requestArguments) match {
            case Success(provider) => provider
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
                           workspaceManagerDAO: WorkspaceManagerDAO,
                           workspaceSettingRepository: WorkspaceSettingRepository,
                           dataRepoDAO: DataRepoDAO,
                           samDAO: SamDAO,
                           bqServiceFactory: GoogleBigQueryServiceFactory,
                           config: DataRepoEntityProviderConfig,
                           cacheEnabled: Boolean,
                           queryTimeout: Duration,
                           metricsPrefix: String
  )(implicit ec: ExecutionContext): EntityManager = {
    // create the EntityManager along with its associated provider-builders. Since entities are only accessed
    // in the context of a workspace, this is safe/correct to do here. We also want to use the same dataSource
    // and execution context for the rawls entity provider that the entity service uses.
    val defaultEntityProviderBuilder =
      new LocalEntityProviderBuilder(dataSource, cacheEnabled, queryTimeout, metricsPrefix) // implicit executionContext
    val dataRepoEntityProviderBuilder = new DataRepoEntityProviderBuilder(workspaceManagerDAO,
                                                                          dataRepoDAO,
                                                                          samDAO,
                                                                          bqServiceFactory,
                                                                          config
    ) // implicit executionContext
    val compactEntityProviderBuilder = new CompactEntityProviderBuilder(dataSource)

    new EntityManager(
      Set(defaultEntityProviderBuilder, dataRepoEntityProviderBuilder, compactEntityProviderBuilder),
      workspaceSettingRepository
    )
  }
}
