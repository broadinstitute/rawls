package org.broadinstitute.dsde.rawls.entities

import org.broadinstitute.dsde.rawls.model.Entity

import scala.concurrent.Future

/**
 * trait definition for entity providers.
 */
trait EntityProvider {

  def createEntity(entity: Entity): Future[Entity]

}
