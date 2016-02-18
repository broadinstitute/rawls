package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.sql.Timestamp
import java.util.UUID

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model._
import slick.dbio.Effect.{Read, Write}
import slick.profile.FixedSqlAction

/**
 * Created by dvoet on 2/4/16.
 */
case class AttributeRecord(id: Long,
                           name: String,
                           valueString: Option[String],
                           valueNumber: Option[Double],
                           valueBoolean: Option[Boolean],
                           valueEntityRef: Option[Long],
                           listIndex: Option[Int])

trait AttributeComponent {
  this: DriverComponent with EntityComponent =>

  import driver.api._

  class AttributeTable(tag: Tag) extends Table[AttributeRecord](tag, "ATTRIBUTE") {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def name = column[String]("name")
    def valueString = column[Option[String]]("value_string")
    def valueNumber = column[Option[Double]]("value_number")
    def valueBoolean = column[Option[Boolean]]("value_boolean")
    def valueEntityRef = column[Option[Long]]("value_entity_ref")
    def listIndex = column[Option[Int]]("list_index")

    def * = (id, name, valueString, valueNumber, valueBoolean, valueEntityRef, listIndex) <> (AttributeRecord.tupled, AttributeRecord.unapply)

    def workspace = foreignKey("FK_ATTRIBUTE_ENTITY_REF", valueEntityRef, entityQuery)(_.id.?)
  }

  protected object attributeQuery extends TableQuery(new AttributeTable(_)) {

    /**
     * Insert an attribute into the database. This will be multiple inserts for a list and will lookup
     * referenced entities.
     *
     * @param name
     * @param attribute
     * @param workspaceId used only for AttributeEntityReferences (or lists of them) to resolve the reference within the workspace
     * @return a sequence of write actions the resulting value being the attribute id inserted
     */
    def insertAttributeRecords(name: String, attribute: Attribute, workspaceId: UUID): Seq[ReadWriteAction[Long]] = {
      attribute match {
        case AttributeEntityReferenceList(refs) =>
          assertConsistentReferenceListMembers(refs)
          refs.zipWithIndex.map { case (ref, index) => insertAttributeRef(name, workspaceId, ref, Option(index))}
        case AttributeValueList(values) =>
          assertConsistentValueListMembers(values)
          values.zipWithIndex.map { case (value, index) => insertAttributeValue(name, value, Option(index))}
        case value: AttributeValue => Seq(insertAttributeValue(name, value))
        case ref: AttributeEntityReference => Seq(insertAttributeRef(name, workspaceId, ref))
        case AttributeEmptyList => Seq(insertAttributeValue(name, AttributeNull, Option(-1))) // storing empty list as an element with index -1
      }
    }

    private def insertAttributeRef(name: String, workspaceId: UUID, ref: AttributeEntityReference, listIndex: Option[Int] = None): ReadWriteAction[Long] = {
      entityQuery.findEntityByName(workspaceId, ref.entityType, ref.entityName).result.flatMap {
        case Seq() => throw new RawlsException(s"$ref not found in workspace $workspaceId")
        case Seq(entityRecord) => (attributeQuery returning attributeQuery.map(_.id)) += marshalAttributeEntityReference(name, listIndex, entityRecord)
      }
    }

    private def insertAttributeValue(name: String, value: AttributeValue, listIndex: Option[Int] = None) = {
      (attributeQuery returning attributeQuery.map(_.id)) += marshalAttributeValue(name, value, listIndex)
    }

    private def marshalAttributeEntityReference(name: String, listIndex: Option[Int], entityRecord: EntityRecord): AttributeRecord = {
      AttributeRecord(0, name, None, None, None, Option(entityRecord.id), listIndex)
    }

    private def marshalAttributeValue(name: String, value: AttributeValue, listIndex: Option[Int]): AttributeRecord = {
      AttributeRecord(0, name, listIndex = listIndex,
        valueBoolean = value match {
          case AttributeBoolean(b) => Option(b)
          case _ => None
        },
        valueNumber = value match {
          case AttributeNumber(b) => Option(b.toDouble)
          case _ => None
        },
        valueString = value match {
          case AttributeString(b) => Option(b)
          case _ => None
        },
        valueEntityRef = None
      )
    }

    def deleteAttributeRecords(attributeRecords: Seq[AttributeRecord]): DBIOAction[Int, NoStream, Write] = {
      filter(_.id inSetBind attributeRecords.map(_.id)).delete
    }

    def unmarshalAttributes(allAttributeRecsWithRef: Seq[(AttributeRecord, Option[EntityRecord])]): Map[String, Attribute] = {
      allAttributeRecsWithRef.groupBy(_._1.name).map { case (name, attributeRecsWithRefForName) =>
        if (attributeRecsWithRefForName.forall(_._1.listIndex.isDefined)) {
          name -> unmarshalList(attributeRecsWithRefForName)
        } else if (attributeRecsWithRefForName.size > 1) {
          throw new RawlsException(s"more than one value exists for attribute $name but list index is not defined for all, records: $attributeRecsWithRefForName")
        } else if (attributeRecsWithRefForName.head._2.isDefined) {
          name -> unmarshalReference(attributeRecsWithRefForName.head._2.get)
        } else {
          name -> unmarshalValue(attributeRecsWithRefForName.head._1)
        }
      }
    }

    private def unmarshalList(attributeRecsWithRef: Seq[(AttributeRecord, Option[EntityRecord])]) = {
      val sortedRecs = attributeRecsWithRef.sortBy(_._1.listIndex.get)
      if (sortedRecs.head._1.listIndex.get == -1) {
        AttributeEmptyList
      } else if (sortedRecs.head._2.isDefined) {
        AttributeEntityReferenceList(sortedRecs.map { case (attributeRec, entityRecOption) =>
          entityRecOption.getOrElse(throw new RawlsException(s"missing entity reference for attribute ${attributeRec}"))
        }.map(unmarshalReference))
      } else {
        AttributeValueList(sortedRecs.map(_._1).map(unmarshalValue))
      }
    }

    private def assertConsistentValueListMembers(attributes: Seq[AttributeValue]): Unit = {
      val headAttribute = attributes.head
      if (!attributes.forall(_.getClass == headAttribute.getClass)) {
        throw new RawlsException(s"inconsistent attributes for list: $attributes")
      }
    }

    private def assertConsistentReferenceListMembers(attributes: Seq[AttributeEntityReference]): Unit = {
      val headAttribute = attributes.head
      if (!attributes.forall(_.entityType == headAttribute.entityType)) {
        throw new RawlsException(s"inconsistent entity types for list: $attributes")
      }
    }

    private def unmarshalValue(attributeRec: AttributeRecord): AttributeValue = {
      if (attributeRec.valueBoolean.isDefined) {
        AttributeBoolean(attributeRec.valueBoolean.get)
      } else if (attributeRec.valueNumber.isDefined) {
        AttributeNumber(attributeRec.valueNumber.get)
      } else if (attributeRec.valueString.isDefined) {
        AttributeString(attributeRec.valueString.get)
      } else {
        AttributeNull
      }
    }

    private def unmarshalReference(referredEntity: EntityRecord): AttributeEntityReference = {
      AttributeEntityReference(entityType = referredEntity.entityType, entityName = referredEntity.name)
    }
  }
}
