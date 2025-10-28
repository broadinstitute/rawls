package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.entities.compact.CompactEntitySerialization
import org.broadinstitute.dsde.rawls.entities.exceptions.DataEntityException
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, AttributeName, Entity, EntityPointer}

import java.util.UUID
import scala.annotation.unused
import scala.util.{Failure, Success, Try}

// unused; helps find this file in IntelliJ searches
@unused
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
  def toPointer: EntityPointer = EntityPointer(entityType, name)
  def toAttributeEntityReference: AttributeEntityReference = AttributeEntityReference(entityType, name)
  def toEntity: Entity = {
    val attrs: AttributeMap = Try(CompactEntitySerialization.fromSql(attributes)) match {
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
case class CompactEntityRefRecord(id: Long, name: String, entityType: String) {
  def toPointer: EntityPointer = EntityPointer(entityType, name)
  def toAttributeEntityReference: AttributeEntityReference = AttributeEntityReference(entityType, name)
}

/**
  * abbreviated model for rows in the ENTITY table when we need the record_version but don't need other columns
  */
case class CompactEntityVersionRecord(id: Long, name: String, entityType: String, recordVersion: Long) {
  def toPointer: EntityPointer = EntityPointer(entityType, name)
  def toAttributeEntityReference: AttributeEntityReference = AttributeEntityReference(entityType, name)
}

/**
  * model class for rows in the ENTITY_REFS table
  */
case class RefPointerRecord(workspaceId: UUID,
                            fromEntityId: Long,
                            fromEntityType: String,
                            fromName: String,
                            fromAttribute: String,
                            toEntityType: String,
                            toName: String
)

/** all reference pointers from one entity to all its reference targets */
case class RefMapping(from: EntityPointer, to: Set[EntityPointer])

case class EntityTypeAndAttributeKey(
  entityType: String,
  attributeKey: AttributeName
)

case class EntityTypeAndAttributeKeys(
  entityType: String,
  attributeKeys: Set[AttributeName]
)

case class EntityTypeAndCount(
  entityType: String,
  count: Int
)
