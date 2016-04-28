package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.sql.Timestamp
import java.util.UUID

import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, RawlsException}
import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import org.broadinstitute.dsde.rawls.model._
import slick.dbio.Effect.{Read, Write}
import slick.profile.FixedSqlAction
import spray.http.StatusCodes

/**
 * Created by dvoet on 2/4/16.
 */
case class EntityAttributeRecord(id: Long,
                           entityId: Long,
                           name: String,
                           valueString: Option[String],
                           valueNumber: Option[Double],
                           valueBoolean: Option[Boolean],
                           valueEntityRef: Option[Long],
                           listIndex: Option[Int])

case class WorkspaceAttributeRecord(id: Long,
                                    workspaceId: UUID,
                                    name: String,
                                    valueString: Option[String],
                                    valueNumber: Option[Double],
                                    valueBoolean: Option[Boolean],
                                    valueEntityRef: Option[Long],
                                    listIndex: Option[Int])

case class SubmissionAttributeRecord(id: Long,
                                       name: String,
                                       valueString: Option[String],
                                       valueNumber: Option[Double],
                                       valueBoolean: Option[Boolean],
                                       valueEntityRef: Option[Long],
                                       listIndex: Option[Int])

trait AttributeComponent {
  this: DriverComponent with EntityComponent with WorkspaceComponent =>

  import driver.api._

  class EntityAttributeTable(tag: Tag) extends Table[EntityAttributeRecord](tag, "ENTITY_ATTRIBUTE") {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def entityId = column[Long]("entity_id")
    def name = column[String]("name")
    def valueString = column[Option[String]]("value_string")
    def valueNumber = column[Option[Double]]("value_number")
    def valueBoolean = column[Option[Boolean]]("value_boolean")
    def valueEntityRef = column[Option[Long]]("value_entity_ref")
    def listIndex = column[Option[Int]]("list_index")

    def * = (id, entityId, name, valueString, valueNumber, valueBoolean, valueEntityRef, listIndex) <> (EntityAttributeRecord.tupled, EntityAttributeRecord.unapply)

    def entityRef = foreignKey("FK_ENT_ATTRIBUTE_ENTITY_REF", valueEntityRef, entityQuery)(_.id.?)
    def parentEntity = foreignKey("FK_ATTRIBUTE_PARENT_ENTITY", entityId, entityQuery)(_.id)
  }

  class WorkspaceAttributeTable(tag: Tag) extends Table[WorkspaceAttributeRecord](tag, "WORKSPACE_ATTRIBUTE") {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def workspaceId = column[UUID]("workspace_id")
    def name = column[String]("name")
    def valueString = column[Option[String]]("value_string")
    def valueNumber = column[Option[Double]]("value_number")
    def valueBoolean = column[Option[Boolean]]("value_boolean")
    def valueEntityRef = column[Option[Long]]("value_entity_ref")
    def listIndex = column[Option[Int]]("list_index")

    def * = (id, workspaceId, name, valueString, valueNumber, valueBoolean, valueEntityRef, listIndex) <> (WorkspaceAttributeRecord.tupled, WorkspaceAttributeRecord.unapply)

    def entityRef = foreignKey("FK_WS_ATTRIBUTE_ENTITY_REF", valueEntityRef, entityQuery)(_.id.?)
    def workspace = foreignKey("FK_ATTRIBUTE_PARENT_WORKSPACE", workspaceId, workspaceQuery)(_.id)
  }

  class SubmissionAttributeTable(tag: Tag) extends Table[SubmissionAttributeRecord](tag, "SUBMISSION_ATTRIBUTE") {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def workspaceId = column[UUID]("workspace_id")
    def name = column[String]("name")
    def valueString = column[Option[String]]("value_string")
    def valueNumber = column[Option[Double]]("value_number")
    def valueBoolean = column[Option[Boolean]]("value_boolean")
    def valueEntityRef = column[Option[Long]]("value_entity_ref")
    def listIndex = column[Option[Int]]("list_index")

    def * = (id, name, valueString, valueNumber, valueBoolean, valueEntityRef, listIndex) <> (SubmissionAttributeRecord.tupled, SubmissionAttributeRecord.unapply)

    def entityRef = foreignKey("FK_SUB_ATTRIBUTE_ENTITY_REF", valueEntityRef, entityQuery)(_.id.?)
  }

  protected object entityAttributeQuery extends TableQuery(new EntityAttributeTable(_)) {

    /**
     * Insert an attribute into the database. This will be multiple inserts for a list and will lookup
     * referenced entities.
     *
     * @param name
     * @param attribute
     * @param workspaceId used only for AttributeEntityReferences (or lists of them) to resolve the reference within the workspace
     * @return a sequence of write actions the resulting value being the attribute id inserted
     */
    def insertAttributeRecords(entityId: Long, name: String, attribute: Attribute, workspaceId: UUID): Seq[ReadWriteAction[Int]] = {
      attribute match {
        case AttributeEntityReferenceList(refs) =>
          assertConsistentReferenceListMembers(refs)
          refs.zipWithIndex.map { case (ref, index) => insertAttributeRef(entityId, name, workspaceId, ref, Option(index))}
        case AttributeValueList(values) =>
          assertConsistentValueListMembers(values)
          values.zipWithIndex.map { case (value, index) => insertAttributeValue(entityId, name, value, Option(index))}
        case value: AttributeValue => Seq(insertAttributeValue(entityId, name, value))
        case ref: AttributeEntityReference => Seq(insertAttributeRef(entityId, name, workspaceId, ref))
        case AttributeEmptyList => Seq(insertAttributeValue(entityId, name, AttributeNull, Option(-1))) // storing empty list as an element with index -1
      }
    }

    private def insertAttributeRef(entityId: Long, name: String, workspaceId: UUID, ref: AttributeEntityReference, listIndex: Option[Int] = None): ReadWriteAction[Int] = {
      entityQuery.findEntityByName(workspaceId, ref.entityType, ref.entityName).result.flatMap {
        case Seq() => throw new RawlsException(s"$ref not found in workspace $workspaceId")
        case Seq(entityRecord) =>
          (entityAttributeQuery += marshalAttributeEntityReference(entityId, name, listIndex, ref, Map(ref -> entityRecord.id)))
      }
    }

    private def insertAttributeValue(entityId: Long, name: String, value: AttributeValue, listIndex: Option[Int] = None): ReadWriteAction[Int] = {
      (entityAttributeQuery += marshalAttributeValue(entityId, name, value, listIndex))
    }

    def marshalAttribute(entityId: Long, name: String, attribute: Attribute, entityIdsByRef: Map[AttributeEntityReference, Long]): Seq[EntityAttributeRecord] = {
      attribute match {
        case AttributeEntityReferenceList(refs) =>
          assertConsistentReferenceListMembers(refs)
          refs.zipWithIndex.map { case (ref, index) => marshalAttributeEntityReference(entityId, name, Option(index), ref, entityIdsByRef)}
        case AttributeValueList(values) =>
          assertConsistentValueListMembers(values)
          values.zipWithIndex.map { case (value, index) => marshalAttributeValue(entityId, name, value, Option(index))}
        case value: AttributeValue => Seq(marshalAttributeValue(entityId, name, value, None))
        case ref: AttributeEntityReference => Seq(marshalAttributeEntityReference(entityId, name, None, ref, entityIdsByRef))
        case AttributeEmptyList => Seq(marshalAttributeValue(entityId, name, AttributeNull, Option(-1))) // storing empty list as an element with index -1
      }
    }

    def batchInsertAttributes(attributes: Seq[EntityAttributeRecord]) = {
      val recordsGrouped = attributes.grouped(batchSize).toSeq
      DBIO.sequence(recordsGrouped map { batch  =>
        (entityAttributeQuery ++= batch)
      })
    }

    private def marshalAttributeEntityReference(parentEntityId: Long, name: String, listIndex: Option[Int], ref: AttributeEntityReference, entityIdsByRef: Map[AttributeEntityReference, Long]): EntityAttributeRecord = {
      val entityId = entityIdsByRef.getOrElse(ref, throw new RawlsException(s"$ref not found"))
      EntityAttributeRecord(0, parentEntityId, name, None, None, None, Option(entityId), listIndex)
    }

    def marshalAttributeValue(entityId: Long, name: String, value: AttributeValue, listIndexx: Option[Int]): EntityAttributeRecord = {
      EntityAttributeRecord(0, entityId, name, listIndex = listIndexx,
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

    def deleteAttributeRecords(attributeRecords: Seq[EntityAttributeRecord]): DBIOAction[Int, NoStream, Write] = {
      filter(_.id inSetBind attributeRecords.map(_.id)).delete
    }

    def unmarshalAttributes[ID](allAttributeRecsWithRef: Seq[((ID, EntityAttributeRecord), Option[EntityRecord])]): Map[ID, Map[String, Attribute]] = {
      allAttributeRecsWithRef.groupBy { case ((id, attrRec), entOp) => id }.mapValues { workspaceAttributeRecsWithRef =>
        workspaceAttributeRecsWithRef.groupBy { case ((id, attrRec), entOp) => attrRec.name }.mapValues { case (attributeRecsWithRefForNameWithDupes) =>
          val attributeRecsWithRefForName: Set[(EntityAttributeRecord, Option[EntityRecord])] = attributeRecsWithRefForNameWithDupes.map { case ((wsId, attributeRec), entityRec) => (attributeRec, entityRec) }.toSet
          if (attributeRecsWithRefForName.forall(_._1.listIndex.isDefined)) {
            unmarshalList(attributeRecsWithRefForName)
          } else if (attributeRecsWithRefForName.size > 1) {
            throw new RawlsException(s"more than one value exists for attribute but list index is not defined for all, records: $attributeRecsWithRefForName")
          } else if (attributeRecsWithRefForName.head._2.isDefined) {
            unmarshalReference(attributeRecsWithRefForName.head._2.get)
          } else {
            unmarshalValue(attributeRecsWithRefForName.head._1)
          }
        }
      }
    }

    private def unmarshalList(attributeRecsWithRef: Set[(EntityAttributeRecord, Option[EntityRecord])]) = {
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

    private def unmarshalValue(attributeRec: EntityAttributeRecord): AttributeValue = {
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

  protected object workspaceAttributeQuery extends TableQuery(new WorkspaceAttributeTable(_)) {

    /**
     * Insert an attribute into the database. This will be multiple inserts for a list and will lookup
     * referenced entities.
     *
     * @param name
     * @param attribute
     * @param workspaceId used only for AttributeEntityReferences (or lists of them) to resolve the reference within the workspace
     * @return a sequence of write actions the resulting value being the attribute id inserted
     */
    def insertAttributeRecords(name: String, attribute: Attribute, workspaceId: UUID): Seq[ReadWriteAction[Int]] = {
      attribute match {
        case AttributeEntityReferenceList(refs) =>
          assertConsistentReferenceListMembers(refs)
          refs.zipWithIndex.map { case (ref, index) => insertAttributeRef(name, workspaceId, ref, Option(index))}
        case AttributeValueList(values) =>
          assertConsistentValueListMembers(values)
          values.zipWithIndex.map { case (value, index) => insertAttributeValue(workspaceId, name, value, Option(index))}
        case value: AttributeValue => Seq(insertAttributeValue(workspaceId, name, value))
        case ref: AttributeEntityReference => Seq(insertAttributeRef(name, workspaceId, ref))
        case AttributeEmptyList => Seq(insertAttributeValue(workspaceId, name, AttributeNull, Option(-1))) // storing empty list as an element with index -1
      }
    }

    private def insertAttributeRef(name: String, workspaceId: UUID, ref: AttributeEntityReference, listIndex: Option[Int] = None): ReadWriteAction[Int] = {
      entityQuery.findEntityByName(workspaceId, ref.entityType, ref.entityName).result.flatMap {
        case Seq() => throw new RawlsException(s"$ref not found in workspace $workspaceId")
        case Seq(entityRecord) =>
          (workspaceAttributeQuery += marshalAttributeEntityReference(workspaceId, name, listIndex, ref, Map(ref -> entityRecord.id)))
      }
    }

    private def insertAttributeValue(workspaceId: UUID, name: String, value: AttributeValue, listIndex: Option[Int] = None): ReadWriteAction[Int] = {
      (workspaceAttributeQuery += marshalAttributeValue(workspaceId, name, value, listIndex))
    }

    def marshalAttribute(workspaceId: UUID, name: String, attribute: Attribute, entityIdsByRef: Map[AttributeEntityReference, Long]): Seq[WorkspaceAttributeRecord] = {
      attribute match {
        case AttributeEntityReferenceList(refs) =>
          assertConsistentReferenceListMembers(refs)
          refs.zipWithIndex.map { case (ref, index) => marshalAttributeEntityReference(workspaceId, name, Option(index), ref, entityIdsByRef)}
        case AttributeValueList(values) =>
          assertConsistentValueListMembers(values)
          values.zipWithIndex.map { case (value, index) => marshalAttributeValue(workspaceId, name, value, Option(index))}
        case value: AttributeValue => Seq(marshalAttributeValue(workspaceId, name, value, None))
        case ref: AttributeEntityReference => Seq(marshalAttributeEntityReference(workspaceId, name, None, ref, entityIdsByRef))
        case AttributeEmptyList => Seq(marshalAttributeValue(workspaceId, name, AttributeNull, Option(-1))) // storing empty list as an element with index -1
      }
    }

    def batchInsertAttributes(attributes: Seq[WorkspaceAttributeRecord]) = {
      val recordsGrouped = attributes.grouped(batchSize).toSeq
      DBIO.sequence(recordsGrouped map { batch  =>
        (workspaceAttributeQuery ++= batch)
      })
    }

    private def marshalAttributeEntityReference(workspaceId: UUID, name: String, listIndex: Option[Int], ref: AttributeEntityReference, entityIdsByRef: Map[AttributeEntityReference, Long]): WorkspaceAttributeRecord = {
      val entityId = entityIdsByRef.getOrElse(ref, throw new RawlsException(s"$ref not found"))
      WorkspaceAttributeRecord(0, workspaceId, name, None, None, None, Option(entityId), listIndex)
    }

    def marshalAttributeValue(workspaceId: UUID, name: String, value: AttributeValue, listIndexx: Option[Int]): WorkspaceAttributeRecord = {
      WorkspaceAttributeRecord(0, workspaceId, name, listIndex = listIndexx,
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

    def deleteAttributeRecords(attributeRecords: Seq[WorkspaceAttributeRecord]): DBIOAction[Int, NoStream, Write] = {
      filter(_.id inSetBind attributeRecords.map(_.id)).delete
    }

    def unmarshalAttributes[ID](allAttributeRecsWithRef: Seq[((ID, WorkspaceAttributeRecord), Option[EntityRecord])]): Map[ID, Map[String, Attribute]] = {
      allAttributeRecsWithRef.groupBy { case ((id, attrRec), entOp) => id }.mapValues { workspaceAttributeRecsWithRef =>
        workspaceAttributeRecsWithRef.groupBy { case ((id, attrRec), entOp) => attrRec.name }.mapValues { case (attributeRecsWithRefForNameWithDupes) =>
          val attributeRecsWithRefForName: Set[(WorkspaceAttributeRecord, Option[EntityRecord])] = attributeRecsWithRefForNameWithDupes.map { case ((wsId, attributeRec), entityRec) => (attributeRec, entityRec) }.toSet
          if (attributeRecsWithRefForName.forall(_._1.listIndex.isDefined)) {
            unmarshalList(attributeRecsWithRefForName)
          } else if (attributeRecsWithRefForName.size > 1) {
            throw new RawlsException(s"more than one value exists for attribute but list index is not defined for all, records: $attributeRecsWithRefForName")
          } else if (attributeRecsWithRefForName.head._2.isDefined) {
            unmarshalReference(attributeRecsWithRefForName.head._2.get)
          } else {
            unmarshalValue(attributeRecsWithRefForName.head._1)
          }
        }
      }
    }

    private def unmarshalList(attributeRecsWithRef: Set[(WorkspaceAttributeRecord, Option[EntityRecord])]) = {
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

    private def unmarshalValue(attributeRec: WorkspaceAttributeRecord): AttributeValue = {
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

  protected object submissionAttributeQuery extends TableQuery(new SubmissionAttributeTable(_)) {

    /**
     * Insert an attribute into the database. This will be multiple inserts for a list and will lookup
     * referenced entities.
     *
     * @param name
     * @param attribute
     * @param workspaceId used only for AttributeEntityReferences (or lists of them) to resolve the reference within the workspace
     * @return a sequence of write actions the resulting value being the attribute id inserted
     */
    def insertAttributeRecords(name: String, attribute: Attribute, workspaceId: UUID): Seq[ReadWriteAction[Int]] = {
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

    private def insertAttributeRef(name: String, workspaceId: UUID, ref: AttributeEntityReference, listIndex: Option[Int] = None): ReadWriteAction[Int] = {
      entityQuery.findEntityByName(workspaceId, ref.entityType, ref.entityName).result.flatMap {
        case Seq() => throw new RawlsException(s"$ref not found in workspace $workspaceId")
        case Seq(entityRecord) =>
          (submissionAttributeQuery += marshalAttributeEntityReference(name, listIndex, ref, Map(ref -> entityRecord.id)))
      }
    }

    private def insertAttributeValue(name: String, value: AttributeValue, listIndex: Option[Int] = None): ReadWriteAction[Int] = {
      (submissionAttributeQuery += marshalAttributeValue(name, value, listIndex))
    }

    def marshalAttribute(name: String, attribute: Attribute, entityIdsByRef: Map[AttributeEntityReference, Long]): Seq[SubmissionAttributeRecord] = {
      attribute match {
        case AttributeEntityReferenceList(refs) =>
          assertConsistentReferenceListMembers(refs)
          refs.zipWithIndex.map { case (ref, index) => marshalAttributeEntityReference(name, Option(index), ref, entityIdsByRef)}
        case AttributeValueList(values) =>
          assertConsistentValueListMembers(values)
          values.zipWithIndex.map { case (value, index) => marshalAttributeValue(name, value, Option(index))}
        case value: AttributeValue => Seq(marshalAttributeValue(name, value, None))
        case ref: AttributeEntityReference => Seq(marshalAttributeEntityReference(name, None, ref, entityIdsByRef))
        case AttributeEmptyList => Seq(marshalAttributeValue(name, AttributeNull, Option(-1))) // storing empty list as an element with index -1
      }
    }

    def batchInsertAttributes(attributes: Seq[SubmissionAttributeRecord]) = {
      val recordsGrouped = attributes.grouped(batchSize).toSeq
      DBIO.sequence(recordsGrouped map { batch  =>
        (submissionAttributeQuery ++= batch)
      })
    }

    private def marshalAttributeEntityReference(name: String, listIndex: Option[Int], ref: AttributeEntityReference, entityIdsByRef: Map[AttributeEntityReference, Long]): SubmissionAttributeRecord = {
      val entityId = entityIdsByRef.getOrElse(ref, throw new RawlsException(s"$ref not found"))
      SubmissionAttributeRecord(0, name, None, None, None, Option(entityId), listIndex)
    }

    def marshalAttributeValue(name: String, value: AttributeValue, listIndex: Option[Int]): SubmissionAttributeRecord = {
      SubmissionAttributeRecord(0, name, listIndex = listIndex,
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

    def deleteAttributeRecords(attributeRecords: Seq[SubmissionAttributeRecord]): DBIOAction[Int, NoStream, Write] = {
      filter(_.id inSetBind attributeRecords.map(_.id)).delete
    }

    def unmarshalAttributes[ID](allAttributeRecsWithRef: Seq[((ID, SubmissionAttributeRecord), Option[EntityRecord])]): Map[ID, Map[String, Attribute]] = {
      allAttributeRecsWithRef.groupBy { case ((id, attrRec), entOp) => id }.mapValues { workspaceAttributeRecsWithRef =>
        workspaceAttributeRecsWithRef.groupBy { case ((id, attrRec), entOp) => attrRec.name }.mapValues { case (attributeRecsWithRefForNameWithDupes) =>
          val attributeRecsWithRefForName: Set[(SubmissionAttributeRecord, Option[EntityRecord])] = attributeRecsWithRefForNameWithDupes.map { case ((wsId, attributeRec), entityRec) => (attributeRec, entityRec) }.toSet
          if (attributeRecsWithRefForName.forall(_._1.listIndex.isDefined)) {
            unmarshalList(attributeRecsWithRefForName)
          } else if (attributeRecsWithRefForName.size > 1) {
            throw new RawlsException(s"more than one value exists for attribute but list index is not defined for all, records: $attributeRecsWithRefForName")
          } else if (attributeRecsWithRefForName.head._2.isDefined) {
            unmarshalReference(attributeRecsWithRefForName.head._2.get)
          } else {
            unmarshalValue(attributeRecsWithRefForName.head._1)
          }
        }
      }
    }

    private def unmarshalList(attributeRecsWithRef: Set[(SubmissionAttributeRecord, Option[EntityRecord])]) = {
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

    private def unmarshalValue(attributeRec: SubmissionAttributeRecord): AttributeValue = {
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
