package org.broadinstitute.dsde.rawls.entities

import org.broadinstitute.dsde.rawls.entities.base.{EntityProvider, EntityProviderBuilder}
import org.broadinstitute.dsde.rawls.entities.datarepo.DataRepoEntityProvider
import org.broadinstitute.dsde.rawls.entities.exceptions.DataEntityException
import org.broadinstitute.dsde.rawls.entities.local.LocalEntityProvider
import org.broadinstitute.dsde.rawls.model.{UserInfo, Workspace}

import scala.reflect.runtime.universe._

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

  def resolveProvider(requestArguments: EntityRequestArguments): EntityProvider = {

    // soon: look up the reference name to ensure it exists.
    // for now, this simplistic logic illustrates the approach: choose the right builder for the job.
    val targetTag = if (requestArguments.dataReference.isDefined) {
      typeTag[DataRepoEntityProvider]
    } else {
      typeTag[LocalEntityProvider]
    }

    providerBuilders.find(_.builds == targetTag) match {
      case None => throw new DataEntityException(s"no entity provider available for ${requestArguments.workspace.toWorkspaceName}")
      case Some(builder) => builder.build(requestArguments)
    }
  }

  // convenience for a likely-common pattern
  def resolveProvider(workspace: Workspace, userInfo: UserInfo): EntityProvider =
    resolveProvider(EntityRequestArguments(workspace, userInfo))

}
