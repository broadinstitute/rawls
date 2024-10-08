package org.broadinstitute.dsde.rawls.entities

import bio.terra.workspace.model.CloudPlatform
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.config.DataRepoEntityProviderConfig
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.DataRepoDAO
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{
  GoogleBigQueryServiceFactory,
  GoogleBigQueryServiceFactoryImpl,
  SamDAO,
  SlickDataSource
}
import org.broadinstitute.dsde.rawls.entities.base.{EntityProvider, EntityProviderBuilder}
import org.broadinstitute.dsde.rawls.entities.datarepo.{DataRepoEntityProvider, DataRepoEntityProviderBuilder}
import org.broadinstitute.dsde.rawls.entities.exceptions.DataEntityException
import org.broadinstitute.dsde.rawls.entities.json.{JsonEntityProvider, JsonEntityProviderBuilder}
import org.broadinstitute.dsde.rawls.entities.local.{LocalEntityProvider, LocalEntityProviderBuilder}
import org.broadinstitute.dsde.rawls.model.{ErrorReport, WorkspaceType}

import java.time.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.runtime.universe._
import scala.util.{Failure, Try}

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
class EntityManager(providerBuilders: Set[EntityProviderBuilder[_ <: EntityProvider]]) {

  def resolveProvider(requestArguments: EntityRequestArguments): Try[EntityProvider] = {

    if (!WorkspaceType.RawlsWorkspace.equals(requestArguments.workspace.workspaceType)) {
      throw new DataEntityException(
        s"This API is disabled for ${CloudPlatform.AZURE} workspaces. Contact support for alternatives."
      )
    }

    // soon: look up the reference name to ensure it exists.
    // for now, this simplistic logic illustrates the approach: choose the right builder for the job.

    // TODO AJ-2008: this is a temporary hack to get JsonEntityProvider working
    val targetTag = (requestArguments.dataReference, requestArguments.workspace) match {
      case (Some(_), _)                         => typeTag[DataRepoEntityProvider]
      case (_, x) if x.name.contains("AJ-2008") => typeTag[JsonEntityProvider]
      case _                                    => typeTag[LocalEntityProvider]
    }

    providerBuilders.find(_.builds == targetTag) match {
      case None =>
        Failure(
          new DataEntityException(s"no entity provider available for ${requestArguments.workspace.toWorkspaceName}")
        )
      case Some(builder) => builder.build(requestArguments)
    }
  }

  /**
    * Convenience function that converts resolveProvider to Future and adds a 400 status code in case of DataEntityException
    * @param entityRequestArguments
    * @param executionContext
    * @return
    */
  def resolveProviderFuture(
    entityRequestArguments: EntityRequestArguments
  )(implicit executionContext: ExecutionContext): Future[EntityProvider] =
    Future.fromTry(resolveProvider(entityRequestArguments)).recoverWith { case regrets: DataEntityException =>
      // bubble up the status code from the DataEntityException
      Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(regrets.code, regrets.getMessage)))
    }
}

object EntityManager {
  def defaultEntityManager(dataSource: SlickDataSource,
                           workspaceManagerDAO: WorkspaceManagerDAO,
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

    val jsonEntityProviderBuilder = new JsonEntityProviderBuilder(dataSource, cacheEnabled, queryTimeout, metricsPrefix)

    new EntityManager(Set(defaultEntityProviderBuilder, dataRepoEntityProviderBuilder, jsonEntityProviderBuilder))
  }
}
