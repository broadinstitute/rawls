package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.sql.Timestamp
import java.util.UUID

import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, RawlsException}
import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import org.broadinstitute.dsde.rawls.model._
import slick.ast.TypedType
import slick.dbio.Effect.{Read, Write}
import slick.profile.FixedSqlAction
import spray.http.StatusCodes

/**
 * Created by dvoet on 2/4/16.
 */
trait AttributeRecord {
  val id: Long
  val name: String
  val valueString: Option[String]
  val valueNumber: Option[Double]
  val valueBoolean: Option[Boolean]
  val valueEntityRef: Option[Long]
  val listIndex: Option[Int]
}

case class EntityAttributeRecord(id: Long,
                                 entityId: Long,
                                 name: String,
                                 valueString: Option[String],
                                 valueNumber: Option[Double],
                                 valueBoolean: Option[Boolean],
                                 valueEntityRef: Option[Long],
                                 listIndex: Option[Int]) extends AttributeRecord

case class WorkspaceAttributeRecord(id: Long,
                                    workspaceId: UUID,
                                    name: String,
                                    valueString: Option[String],
                                    valueNumber: Option[Double],
                                    valueBoolean: Option[Boolean],
                                    valueEntityRef: Option[Long],
                                    listIndex: Option[Int]) extends AttributeRecord

case class SubmissionAttributeRecord(id: Long,
                                     validationId: Long,
                                     name: String,
                                     valueString: Option[String],
                                     valueNumber: Option[Double],
                                     valueBoolean: Option[Boolean],
                                     valueEntityRef: Option[Long],
                                     listIndex: Option[Int]) extends AttributeRecord

trait AttributeComponent {
  this: DriverComponent
    with EntityComponent
    with WorkspaceComponent
    with SubmissionComponent =>

  import driver.api._

  abstract class AttributeTable[OWNER_ID: TypedType, RECORD <: AttributeRecord](tag: Tag, tableName: String) extends Table[RECORD](tag, tableName) {
    final type OwnerIdType = OWNER_ID

    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def ownerId = column[OWNER_ID]("owner_id")
    def name = column[String]("name")
    def valueString = column[Option[String]]("value_string")
    def valueNumber = column[Option[Double]]("value_number")
    def valueBoolean = column[Option[Boolean]]("value_boolean")
    def valueEntityRef = column[Option[Long]]("value_entity_ref")
    def listIndex = column[Option[Int]]("list_index")
  }

  class EntityAttributeTable(tag: Tag) extends AttributeTable[Long, EntityAttributeRecord](tag, "ENTITY_ATTRIBUTE") {
    def * = (id, ownerId, name, valueString, valueNumber, valueBoolean, valueEntityRef, listIndex) <> (EntityAttributeRecord.tupled, EntityAttributeRecord.unapply)

    def entityRef = foreignKey("FK_ENT_ATTRIBUTE_ENTITY_REF", valueEntityRef, entityQuery)(_.id.?)
    def parentEntity = foreignKey("FK_ATTRIBUTE_PARENT_ENTITY", ownerId, entityQuery)(_.id)
  }

  class WorkspaceAttributeTable(tag: Tag) extends AttributeTable[UUID, WorkspaceAttributeRecord](tag, "WORKSPACE_ATTRIBUTE") {
    def * = (id, ownerId, name, valueString, valueNumber, valueBoolean, valueEntityRef, listIndex) <> (WorkspaceAttributeRecord.tupled, WorkspaceAttributeRecord.unapply)

    def entityRef = foreignKey("FK_WS_ATTRIBUTE_ENTITY_REF", valueEntityRef, entityQuery)(_.id.?)
    def workspace = foreignKey("FK_ATTRIBUTE_PARENT_WORKSPACE", ownerId, workspaceQuery)(_.id)
  }

  class SubmissionAttributeTable(tag: Tag) extends AttributeTable[Long, SubmissionAttributeRecord](tag, "SUBMISSION_ATTRIBUTE") {
    def * = (id, ownerId, name, valueString, valueNumber, valueBoolean, valueEntityRef, listIndex) <> (SubmissionAttributeRecord.tupled, SubmissionAttributeRecord.unapply)

    def entityRef = foreignKey("FK_SUB_ATTRIBUTE_ENTITY_REF", valueEntityRef, entityQuery)(_.id.?)
    def submissionValidation = foreignKey("FK_ATTRIBUTE_PARENT_SUB_VALIDATION", ownerId, submissionValidationQuery)(_.id)
  }

  protected object entityAttributeQuery extends AttributeQuery[Long, EntityAttributeRecord, EntityAttributeTable](new EntityAttributeTable(_)) {
    override protected def createRecord(id: Long, entityId: Long, name: String, valueString: Option[String], valueNumber: Option[Double], valueBoolean: Option[Boolean], valueEntityRef: Option[Long], listIndex: Option[Int]) =
      EntityAttributeRecord(id, entityId, name, valueString, valueNumber, valueBoolean, valueEntityRef, listIndex)
  }

  protected object workspaceAttributeQuery extends AttributeQuery[UUID, WorkspaceAttributeRecord, WorkspaceAttributeTable](new WorkspaceAttributeTable(_)) {
    override protected def createRecord(id: Long, workspaceId: UUID, name: String, valueString: Option[String], valueNumber: Option[Double], valueBoolean: Option[Boolean], valueEntityRef: Option[Long], listIndex: Option[Int]) =
      WorkspaceAttributeRecord(id, workspaceId, name, valueString, valueNumber, valueBoolean, valueEntityRef, listIndex)
  }

  protected object submissionAttributeQuery extends AttributeQuery[Long, SubmissionAttributeRecord, SubmissionAttributeTable](new SubmissionAttributeTable(_)) {
    override protected def createRecord(id: Long, validationId: Long, name: String, valueString: Option[String], valueNumber: Option[Double], valueBoolean: Option[Boolean], valueEntityRef: Option[Long], listIndex: Option[Int]) =
      SubmissionAttributeRecord(id, validationId, name, valueString, valueNumber, valueBoolean, valueEntityRef, listIndex)
  }

  protected abstract class AttributeQuery[OWNER_ID, RECORD <: AttributeRecord, T <: AttributeTable[OWNER_ID, RECORD]](cons: Tag => T) extends TableQuery[T](cons)  {

    /**
     * Insert an attribute into the database. This will be multiple inserts for a list and will lookup
     * referenced entities.
     *
     * @param name
     * @param attribute
     * @param workspaceId used only for AttributeEntityReferences (or lists of them) to resolve the reference within the workspace
     * @return a sequence of write actions the resulting value being the attribute id inserted
     */
    def insertAttributeRecords(ownerId: OWNER_ID, name: String, attribute: Attribute, workspaceId: UUID): Seq[ReadWriteAction[Int]] = {
      attribute match {
        case AttributeEntityReferenceList(refs) =>
          assertConsistentReferenceListMembers(refs)
          refs.zipWithIndex.map { case (ref, index) => insertAttributeRef(ownerId, name, workspaceId, ref, Option(index))}
        case AttributeValueList(values) =>
          assertConsistentValueListMembers(values)
          values.zipWithIndex.map { case (value, index) => insertAttributeValue(ownerId, name, value, Option(index))}
        case value: AttributeValue => Seq(insertAttributeValue(ownerId, name, value))
        case ref: AttributeEntityReference => Seq(insertAttributeRef(ownerId, name, workspaceId, ref))
        case AttributeEmptyList => Seq(insertAttributeValue(ownerId, name, AttributeNull, Option(-1))) // storing empty list as an element with index -1
      }
    }

    private def insertAttributeRef(ownerId: OWNER_ID, name: String, workspaceId: UUID, ref: AttributeEntityReference, listIndex: Option[Int] = None): ReadWriteAction[Int] = {
      entityQuery.findEntityByName(workspaceId, ref.entityType, ref.entityName).result.flatMap {
        case Seq() => throw new RawlsException(s"$ref not found in workspace $workspaceId")
        case Seq(entityRecord) =>
          (this += marshalAttributeEntityReference(ownerId, name, listIndex, ref, Map(ref -> entityRecord.id)))
      }
    }

    private def insertAttributeValue(ownerId: OWNER_ID, name: String, value: AttributeValue, listIndex: Option[Int] = None): ReadWriteAction[Int] = {
      (this += marshalAttributeValue(ownerId, name, value, listIndex))
    }

    def marshalAttribute(ownerId: OWNER_ID, name: String, attribute: Attribute, entityIdsByRef: Map[AttributeEntityReference, Long]): Seq[T#TableElementType] = {
      attribute match {
        case AttributeEntityReferenceList(refs) =>
          assertConsistentReferenceListMembers(refs)
          refs.zipWithIndex.map { case (ref, index) => marshalAttributeEntityReference(ownerId, name, Option(index), ref, entityIdsByRef)}
        case AttributeValueList(values) =>
          assertConsistentValueListMembers(values)
          values.zipWithIndex.map { case (value, index) => marshalAttributeValue(ownerId, name, value, Option(index))}
        case value: AttributeValue => Seq(marshalAttributeValue(ownerId, name, value, None))
        case ref: AttributeEntityReference => Seq(marshalAttributeEntityReference(ownerId, name, None, ref, entityIdsByRef))
        case AttributeEmptyList => Seq(marshalAttributeValue(ownerId, name, AttributeNull, Option(-1))) // storing empty list as an element with index -1
      }
    }

    def batchInsertAttributes(attributes: Seq[RECORD]) = {
      val recordsGrouped = attributes.grouped(batchSize).toSeq
      DBIO.sequence(recordsGrouped map { batch  =>
        (this ++= batch)
      })
    }

    def marshalAttributeEntityReference(ownerId: OWNER_ID, name: String, listIndex: Option[Int], ref: AttributeEntityReference, entityIdsByRef: Map[AttributeEntityReference, Long]): RECORD = {
      val entityId = entityIdsByRef.getOrElse(ref, throw new RawlsException(s"$ref not found"))
      createRecord(0, ownerId, name, None, None, None, Option(entityId), listIndex)
    }

    def marshalAttributeValue(ownerId: OWNER_ID, name: String, value: AttributeValue, listIndexx: Option[Int]): RECORD = {
      createRecord(0, ownerId, name, listIndex = listIndexx,
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

    def deleteAttributeRecords(attributeRecords: Seq[RECORD]): DBIOAction[Int, NoStream, Write] = {
      filter(_.id inSetBind attributeRecords.map(_.id)).delete
    }

    def unmarshalAttributes[ID](allAttributeRecsWithRef: Seq[((ID, RECORD), Option[EntityRecord])]): Map[ID, Map[String, Attribute]] = {
      allAttributeRecsWithRef.groupBy { case ((id, attrRec), entOp) => id }.map { case (id, workspaceAttributeRecsWithRef) =>
        id -> workspaceAttributeRecsWithRef.groupBy { case ((id, attrRec), entOp) => attrRec.name }.map { case (name, attributeRecsWithRefForNameWithDupes) =>
          val attributeRecsWithRefForName: Set[(RECORD, Option[EntityRecord])] = attributeRecsWithRefForNameWithDupes.map { case ((wsId, attributeRec), entityRec) => (attributeRec, entityRec) }.toSet
          val unmarshalled = if (attributeRecsWithRefForName.forall(_._1.listIndex.isDefined)) {
            unmarshalList(attributeRecsWithRefForName)
          } else if (attributeRecsWithRefForName.size > 1) {
            throw new RawlsException(s"more than one value exists for attribute but list index is not defined for all, records: $attributeRecsWithRefForName")
          } else if (attributeRecsWithRefForName.head._2.isDefined) {
            unmarshalReference(attributeRecsWithRefForName.head._2.get)
          } else {
            unmarshalValue(attributeRecsWithRefForName.head._1)
          }
          name -> unmarshalled
        }
      }
    }

    private def unmarshalList(attributeRecsWithRef: Set[(RECORD, Option[EntityRecord])]) = {
      val sortedRecs = attributeRecsWithRef.toSeq.sortBy(_._1.listIndex.get)
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
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"inconsistent attributes for list: $attributes"))
      }
    }

    private def assertConsistentReferenceListMembers(attributes: Seq[AttributeEntityReference]): Unit = {
      val headAttribute = attributes.head
      if (!attributes.forall(_.entityType == headAttribute.entityType)) {
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"inconsistent entity types for list: $attributes"))
      }
    }

    private def unmarshalValue(attributeRec: RECORD): AttributeValue = {
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

    protected def createRecord(id: Long, ownerId: OWNER_ID, name: String, valueString: Option[String], valueNumber: Option[Double], valueBoolean: Option[Boolean], valueEntityRef: Option[Long], listIndex: Option[Int]): RECORD
  }
}
