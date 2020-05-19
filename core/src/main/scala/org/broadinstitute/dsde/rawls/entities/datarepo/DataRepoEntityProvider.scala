package org.broadinstitute.dsde.rawls.entities.datarepo

import org.broadinstitute.dsde.rawls.entities.base.EntityProvider
import org.broadinstitute.dsde.rawls.entities.exceptions.UnsupportedEntityOperationException
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, Entity, EntityTypeMetadata}

import scala.concurrent.Future

class DataRepoEntityProvider extends EntityProvider {

  override def entityTypeMetadata(): Future[Map[String, EntityTypeMetadata]] = {
    // TODO: contact WSM to retrieve the data reference specified in the request
    // TODO: extract the TDR snapshot ID from the WSM response
    // TODO: contact TDR to describe the snapshot
    // TODO: reformat TDR's response into the expected response structure
    throw new UnsupportedEntityOperationException("type metadata will be supported by this provider, but is not implemented yet")
  }

  override def createEntity(entity: Entity): Future[Entity] =
    throw new UnsupportedEntityOperationException("create entity not supported by this provider.")

  override def deleteEntities(entityRefs: Seq[AttributeEntityReference]): Future[Int] =
    throw new UnsupportedEntityOperationException("delete entities not supported by this provider.")
}
