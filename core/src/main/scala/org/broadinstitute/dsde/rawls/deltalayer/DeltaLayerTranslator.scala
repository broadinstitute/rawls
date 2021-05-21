package org.broadinstitute.dsde.rawls.deltalayer

import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AddUpdateAttribute, AttributeUpdateOperation, EntityUpdateDefinition}

object DeltaLayerTranslator extends LazyLogging {

  /**
   * Inspects the incoming EntityUpdateDefinitions and throws exceptions if any of those updates
   * are illegal.
   * @param entityUpdates the updates to validate
   * @return the original set of updates, if all are valid
   */
  def validateEntityUpdates(entityUpdates: Seq[EntityUpdateDefinition]): Seq[EntityUpdateDefinition] = {
    val allOps: Seq[AttributeUpdateOperation] = entityUpdates flatMap { upd => upd.operations }

    if (allOps.isEmpty)
      throw new DeltaLayerException("This method requires at least one AddUpdateAttribute operation.",
        code = StatusCodes.BadRequest)

    val (upserts, others) = allOps.partition {
      case _:AddUpdateAttribute => true
      case _ => false
    }

    if (others.nonEmpty)
      // TODO: provide more info such as entity type/name and illegal operation?
      throw new DeltaLayerException("This method only accepts AddUpdateAttribute operations.",
        code = StatusCodes.BadRequest)

    if (upserts.isEmpty)
      // how did we reach here? This should be impossible
      throw new DeltaLayerException("This method requires at least one AddUpdateAttribute operation.",
        code = StatusCodes.BadRequest)

    // TODO: do we need to validate values, such as to exclude entityreferences, rawjson, etc?

    entityUpdates
  }



}
