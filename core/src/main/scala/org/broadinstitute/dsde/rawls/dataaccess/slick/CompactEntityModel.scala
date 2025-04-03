package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.entities.exceptions.DataEntityException
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.Entity
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.util.UUID
import scala.util.{Failure, Success, Try}

object CompactEntityModel {}

/**
  * model class for rows in the ENTITY table, used for both raw SQL and high-level Slick operations
  */
case class CompactEntityRecord(id: Long,
                               name: String,
                               entityType: String,
                               workspaceId: UUID,
                               recordVersion: Long,
                               deleted: Boolean,
                               attributes: Option[String]
) {
  def toEntity: Entity = {
    val attrs: AttributeMap = Try(attributes.getOrElse("{}").parseJson.convertTo[AttributeMap]) match {
      case Success(attrMap) => attrMap
      case Failure(ex)      =>
        // best-effort attempt to sanely truncate the error message and not include the entire payload
        val errMsg = ex.getMessage.split('{').head
        throw new DataEntityException(s"Error parsing attribute json for entity $entityType/$name: $errMsg")
    }
    Entity(name, entityType, attrs)
  }
}

/**
  * abbreviated model for rows in the ENTITY table when we don't need all the columns
  */
case class CompactEntityRefRecord(id: Long, name: String, entityType: String)

/**
  * model class for rows in the ENTITY_REFS table
  */
case class RefPointerRecord(fromId: Long, toId: Long)
