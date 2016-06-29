package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.sql.Timestamp
import java.util.UUID

import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, RawlsException}
import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import org.broadinstitute.dsde.rawls.model._
import slick.ast.TypedType
import slick.dbio.Effect.{Read, Write}
import slick.driver.JdbcDriver
import slick.profile.FixedSqlAction
import spray.http.StatusCodes
import reflect.runtime.universe._

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

  class EntityAttributeTempTable(tag: Tag) extends AttributeTable[Long, EntityAttributeRecord](tag, "ENTITY_ATTRIBUTE_TEMP") {
    def * = (id, ownerId, name, valueString, valueNumber, valueBoolean, valueEntityRef, listIndex) <> (EntityAttributeRecord.tupled, EntityAttributeRecord.unapply)
  }

  class WorkspaceAttributeTempTable(tag: Tag) extends AttributeTable[UUID, WorkspaceAttributeRecord](tag, "WORKSPACE_ATTRIBUTE_TEMP") {
    def * = (id, ownerId, name, valueString, valueNumber, valueBoolean, valueEntityRef, listIndex) <> (WorkspaceAttributeRecord.tupled, WorkspaceAttributeRecord.unapply)
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

  protected object entityAttributeTempQuery extends AttributeQuery[Long, EntityAttributeRecord, EntityAttributeTempTable](new EntityAttributeTempTable(_)) {
    override protected def createRecord(id: Long, entityId: Long, name: String, valueString: Option[String], valueNumber: Option[Double], valueBoolean: Option[Boolean], valueEntityRef: Option[Long], listIndex: Option[Int]) =
      EntityAttributeRecord(id, entityId, name, valueString, valueNumber, valueBoolean, valueEntityRef, listIndex)
  }

  protected object workspaceAttributeTempQuery extends AttributeQuery[UUID, WorkspaceAttributeRecord, WorkspaceAttributeTempTable](new WorkspaceAttributeTempTable(_)) {
    override protected def createRecord(id: Long, workspaceId: UUID, name: String, valueString: Option[String], valueNumber: Option[Double], valueBoolean: Option[Boolean], valueEntityRef: Option[Long], listIndex: Option[Int]) =
      WorkspaceAttributeRecord(id, workspaceId, name, valueString, valueNumber, valueBoolean, valueEntityRef, listIndex)
  }

  protected abstract class AttributeQuery[OWNER_ID: TypeTag, RECORD <: AttributeRecord, T <: AttributeTable[OWNER_ID, RECORD]](cons: Tag => T) extends TableQuery[T](cons)  {

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
      insertInBatches(this, attributes)
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

    object AlterAttributesUsingTempTableQueries extends RawSqlQuery {
      val driver: JdbcDriver = AttributeComponent.this.driver
      /*!CAUTION! - do not drop the ownerIds clause at the end or it will delete every attribute in the table!

        deleteMasterFromAction: deletes any row in *_ATTRIBUTE that doesn't
          exist in *_ATTRIBUTE_TEMP but exists in *_ATTRIBUTE (bound by ownerIds)
        updateInMasterAction: updates any row in *_ATTRIBUTE that also
          exists in *_ATTRIBUTE_TEMP (bound by ownerIds)
        insertIntoMasterAction: inserts any row into *_ATTRIBUTE that doesn't
          exist in *_ATTRIBUTE but exists in *_ATTRIBUTE_TEMP (bound by ownerIds)

        These left joins give us the rows related to the attributes that are in the temp table.
        We filter on ta.owner_id == null if we want to see that the row does NOT exist in the temp table
        We filter on a.owner_id == null if we want to see that the row does NOT exist in the real table
        We filter on ta.owner_id != null if we want to see that the row exists in both of the tables
       */

      def ownerIdTail(ownerIds: Seq[OWNER_ID]) = {
        concatSqlActions(sql"(", reduceSqlActionsWithDelim(ownerIds.map {
          case(ownerId: UUID) => sql"$ownerId"
          case(ownerId: Long) => sql"$ownerId"
        }), sql")")
      }

      //MySQL seems to handle null safe operators inefficiently. the solution to this
      //is to use ifnull and default to -2 if the list_index is null. list_index of -2
      //is never used otherwise
      def deleteFromMasterAction(ownerIds: Seq[OWNER_ID]) =
        concatSqlActions(sql"""delete a from #${baseTableRow.tableName} a
                left join #${baseTableRow.tableName}_TEMP ta
                on (a.name,a.owner_id,ifnull(a.list_index,-2))=(ta.name,ta.owner_id,ifnull(ta.list_index,-2))
                where ta.owner_id is null and a.owner_id in """, ownerIdTail(ownerIds)).as[Int]
      def updateInMasterAction(ownerIds: Seq[OWNER_ID]) =
        sql"""update #${baseTableRow.tableName} a
                join #${baseTableRow.tableName}_TEMP ta
                on (a.name,a.owner_id,ifnull(a.list_index,-2))=(ta.name,ta.owner_id,ifnull(ta.list_index,-2))
                set a.value_string=ta.value_string, a.value_number=ta.value_number, a.value_boolean=ta.value_boolean, a.value_entity_ref=ta.value_entity_ref""".as[Int]
      def insertIntoMasterAction(ownerIds: Seq[OWNER_ID]) =
        concatSqlActions(sql"""insert into #${baseTableRow.tableName}(name,value_string,value_number,value_boolean,value_entity_ref,list_index,owner_id)
                select ta.name,ta.value_string,ta.value_number,ta.value_boolean,ta.value_entity_ref,ta.list_index,ta.owner_id
                from #${baseTableRow.tableName}_TEMP ta
                left join #${baseTableRow.tableName} a
                on (a.name,a.owner_id,a.list_index)<=>(ta.name,ta.owner_id,ta.list_index) and a.owner_id in """, ownerIdTail(ownerIds),
                sql""" where a.owner_id is null""").as[Int]

      def createAttributeTempTableAction() = {
        val prefix = sql"""create temporary table #${baseTableRow.tableName}_TEMP (
              id bigint(20) unsigned NOT NULL AUTO_INCREMENT primary key,
              name text NOT NULL,
              value_string text,
              value_number double DEFAULT NULL,
              value_boolean bit(1) DEFAULT NULL,
              value_entity_ref bigint(20) unsigned DEFAULT NULL,
              list_index int(11) DEFAULT NULL, """

        val suffix = if(typeOf[OWNER_ID] =:= typeOf[Long]) {
          sql"""owner_id bigint(20) unsigned NOT NULL)"""
        } else if(typeOf[OWNER_ID] =:= typeOf[UUID]) {
          sql"""owner_id binary(16) NOT NULL)"""
        } else throw new RawlsException("Owner ID was of an unexpected type")

        concatSqlActions(prefix, suffix).as[Int]
      }

      def dropAttributeTempTableAction() = {
        sql"""drop temporary table if exists #${baseTableRow.tableName}_TEMP""".as[Int]
      }

      def upsertAction(ownerIds: Seq[OWNER_ID], insertFunction: () => ReadWriteAction[Unit]) = {
        dropAttributeTempTableAction() andThen
          createAttributeTempTableAction() andThen
          insertFunction() andThen
          deleteFromMasterAction(ownerIds) andThen
          updateInMasterAction(ownerIds) andThen
          insertIntoMasterAction(ownerIds) andFinally
          dropAttributeTempTableAction()
      }
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
      if(!attributes.isEmpty) {
        val headAttribute = attributes.head
        if (!attributes.forall(_.getClass == headAttribute.getClass)) {
          throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"inconsistent attributes for list: $attributes"))
        }
      }
    }

    private def assertConsistentReferenceListMembers(attributes: Seq[AttributeEntityReference]): Unit = {
      if(!attributes.isEmpty) {
        val headAttribute = attributes.head
        if (!attributes.forall(_.entityType == headAttribute.entityType)) {
          throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"inconsistent entity types for list: $attributes"))
        }
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
