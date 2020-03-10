package org.broadinstitute.dsde.rawls.entities

import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, Entity, Workspace}

import scala.concurrent.Future
import scala.reflect.runtime.universe._

/**
 * Requests for entity operations enter here. This is the interface that calling code should use; calling
 * code should not directly access entity providers.
 * This manager will inspect the request and route it to the appropriate provider.
 */
class EntityManager(providerBuilders: Set[EntityProviderBuilder[_ <: EntityProvider]]) {

  /** create the supplied entity inside the supplied workspace.
   */
  def createEntity(workspace: Workspace, entity: Entity): Future[Entity] = {
    resolveProvider(workspace).createEntity(entity)
  }

  // soon: this interface will have multiple other methods, for example:
  def getEntity(workspace: Workspace, entityRef: AttributeEntityReference) = ???

  // internal method to figure out which entityprovider should be used for a given request
  private def resolveProvider(workspace: Workspace): EntityProvider = {
    // soon: inspect the workspace's registered snapshots plus request parameters,
    // and route to the appropriate entity provider. This will likely require a method signature change
    // to accept request details. For now, just find the builder for DefaultEntityProvider.
    val defaultEntityProviderBuilder = providerBuilders.find(_.builds == typeTag[RawlsEntityProvider])

    defaultEntityProviderBuilder match {
      case None => throw new DataEntityException(s"no entity provider available for ${workspace.toWorkspaceName}")
      case Some(builder) =>builder.build(workspace)
    }
  }

}
