package org.broadinstitute.dsde.rawls.entities.datarepo

import org.broadinstitute.dsde.rawls.entities.base.EntityProvider
import org.broadinstitute.dsde.rawls.entities.exceptions.UnsupportedEntityOperationException
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, Entity}

import scala.concurrent.Future

class DataRepoEntityProvider extends EntityProvider {
  override def createEntity(entity: Entity): Future[Entity] =
    throw new UnsupportedEntityOperationException("create entity not supported by this provider.")

  override def deleteEntities(entityRefs: Seq[AttributeEntityReference]): Future[Int] =
    throw new UnsupportedEntityOperationException("delete entities not supported by this provider.")
}
