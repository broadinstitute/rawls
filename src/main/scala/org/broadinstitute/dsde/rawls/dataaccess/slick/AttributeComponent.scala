package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.util.UUID

import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model._
import slick.ast.TypedType
import slick.dbio.Effect.Write
import slick.driver.JdbcDriver
import spray.http.StatusCodes

import reflect.runtime.universe._

/**
 * Created by dvoet on 2/4/16.
 */
trait AttributeRecord[OWNER_ID] {
  val id: Long
  val ownerId: OWNER_ID
  val namespace: String
  val name: String
  val valueString: Option[String]
  val valueNumber: Option[Double]
  val valueBoolean: Option[Boolean]
  val valueEntityRef: Option[Long]
  val listIndex: Option[Int]
  val listLength: Option[Int]
}

trait AttributeScratchRecord[OWNER_ID] extends AttributeRecord[OWNER_ID] {
  val transactionId: String
}

case class EntityAttributeRecord(id: Long,
                                 ownerId: Long, // entity id
                                 namespace: String,
                                 name: String,
                                 valueString: Option[String],
                                 valueNumber: Option[Double],
                                 valueBoolean: Option[Boolean],
                                 valueEntityRef: Option[Long],
                                 listIndex: Option[Int],
                                 listLength: Option[Int]) extends AttributeRecord[Long]

case class EntityAttributeScratchRecord(id: Long,
                                     ownerId: Long, // entity id
                                     namespace: String,
                                     name: String,
                                     valueString: Option[String],
                                     valueNumber: Option[Double],
                                     valueBoolean: Option[Boolean],
                                     valueEntityRef: Option[Long],
                                     listIndex: Option[Int],
                                     listLength: Option[Int],
                                     transactionId: String) extends AttributeScratchRecord[Long] {
}

case class WorkspaceAttributeRecord(id: Long,
                                    ownerId: UUID, // workspace id
                                    namespace: String,
                                    name: String,
                                    valueString: Option[String],
                                    valueNumber: Option[Double],
                                    valueBoolean: Option[Boolean],
                                    valueEntityRef: Option[Long],
                                    listIndex: Option[Int],
                                    listLength: Option[Int]) extends AttributeRecord[UUID]

case class WorkspaceAttributeScratchRecord(id: Long,
                                        ownerId: UUID, // workspace id
                                        namespace: String,
                                        name: String,
                                        valueString: Option[String],
                                        valueNumber: Option[Double],
                                        valueBoolean: Option[Boolean],
                                        valueEntityRef: Option[Long],
                                        listIndex: Option[Int],
                                        listLength: Option[Int],
                                        transactionId: String) extends AttributeScratchRecord[UUID]

case class SubmissionAttributeRecord(id: Long,
                                     ownerId: Long, // validation id
                                     namespace: String,
                                     name: String,
                                     valueString: Option[String],
                                     valueNumber: Option[Double],
                                     valueBoolean: Option[Boolean],
                                     valueEntityRef: Option[Long],
                                     listIndex: Option[Int],
                                     listLength: Option[Int]) extends AttributeRecord[Long]

trait AttributeComponent {
  this: DriverComponent
    with EntityComponent
    with WorkspaceComponent
    with SubmissionComponent =>

  import driver.api._

  //Magic const we put in listIndex to indicate the dummy row is a list type.
  val EMPTY_LIST_INDEX_MAGIC = -1

  def isEntityRefRecord[T](rec: AttributeRecord[T]): Boolean = {
    val isEmptyRefListDummyRecord = rec.listIndex.isDefined && rec.listIndex.get == -1 && rec.valueNumber.isEmpty
    rec.valueEntityRef.isDefined || isEmptyRefListDummyRecord
  }

  abstract class AttributeTable[OWNER_ID: TypedType, RECORD <: AttributeRecord[OWNER_ID]](tag: Tag, tableName: String) extends Table[RECORD](tag, tableName) {
    final type OwnerIdType = OWNER_ID

    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def ownerId = column[OWNER_ID]("owner_id")
    def namespace = column[String]("namespace", O.Length(32))
    def name = column[String]("name")
    def valueString = column[Option[String]]("value_string")
    def valueNumber = column[Option[Double]]("value_number")
    def valueBoolean = column[Option[Boolean]]("value_boolean")
    def valueEntityRef = column[Option[Long]]("value_entity_ref")
    def listIndex = column[Option[Int]]("list_index")
    def listLength = column[Option[Int]]("list_length")
  }

  abstract class AttributeScratchTable[OWNER_ID: TypedType, RECORD <: AttributeScratchRecord[OWNER_ID]](tag: Tag, tableName: String) extends AttributeTable[OWNER_ID, RECORD](tag, tableName) {
    def transactionId = column[String]("transaction_id")
  }

  class EntityAttributeTable(tag: Tag) extends AttributeTable[Long, EntityAttributeRecord](tag, "ENTITY_ATTRIBUTE") {
    def * = (id, ownerId, namespace, name, valueString, valueNumber, valueBoolean, valueEntityRef, listIndex, listLength) <> (EntityAttributeRecord.tupled, EntityAttributeRecord.unapply)

    def uniqueIdx = index("UNQ_ENTITY_ATTRIBUTE", (ownerId, namespace, name, listIndex), unique = true)

    def entityRef = foreignKey("FK_ENT_ATTRIBUTE_ENTITY_REF", valueEntityRef, entityQuery)(_.id.?)
    def parentEntity = foreignKey("FK_ATTRIBUTE_PARENT_ENTITY", ownerId, entityQuery)(_.id)
  }

  class WorkspaceAttributeTable(tag: Tag) extends AttributeTable[UUID, WorkspaceAttributeRecord](tag, "WORKSPACE_ATTRIBUTE") {
    def * = (id, ownerId, namespace, name, valueString, valueNumber, valueBoolean, valueEntityRef, listIndex, listLength) <> (WorkspaceAttributeRecord.tupled, WorkspaceAttributeRecord.unapply)

    def uniqueIdx = index("UNQ_WORKSPACE_ATTRIBUTE", (ownerId, namespace, name, listIndex), unique = true)

    def entityRef = foreignKey("FK_WS_ATTRIBUTE_ENTITY_REF", valueEntityRef, entityQuery)(_.id.?)
    def workspace = foreignKey("FK_ATTRIBUTE_PARENT_WORKSPACE", ownerId, workspaceQuery)(_.id)
  }

  class SubmissionAttributeTable(tag: Tag) extends AttributeTable[Long, SubmissionAttributeRecord](tag, "SUBMISSION_ATTRIBUTE") {
    def * = (id, ownerId, namespace, name, valueString, valueNumber, valueBoolean, valueEntityRef, listIndex, listLength) <> (SubmissionAttributeRecord.tupled, SubmissionAttributeRecord.unapply)

    def uniqueIdx = index("UNQ_SUBMISSION_ATTRIBUTE", (ownerId, namespace, name, listIndex), unique = true)

    def entityRef = foreignKey("FK_SUB_ATTRIBUTE_ENTITY_REF", valueEntityRef, entityQuery)(_.id.?)
    def submissionValidation = foreignKey("FK_ATTRIBUTE_PARENT_SUB_VALIDATION", ownerId, submissionValidationQuery)(_.id)
  }

  class EntityAttributeScratchTable(tag: Tag) extends AttributeScratchTable[Long, EntityAttributeScratchRecord](tag, "ENTITY_ATTRIBUTE_SCRATCH") {
    def * = (id, ownerId, namespace, name, valueString, valueNumber, valueBoolean, valueEntityRef, listIndex, listLength, transactionId) <> (EntityAttributeScratchRecord.tupled, EntityAttributeScratchRecord.unapply)
  }

  class WorkspaceAttributeScratchTable(tag: Tag) extends AttributeScratchTable[UUID, WorkspaceAttributeScratchRecord](tag, "WORKSPACE_ATTRIBUTE_SCRATCH") {
    def * = (id, ownerId, namespace, name, valueString, valueNumber, valueBoolean, valueEntityRef, listIndex, listLength, transactionId) <>(WorkspaceAttributeScratchRecord.tupled, WorkspaceAttributeScratchRecord.unapply)
  }

  protected object entityAttributeQuery extends AttributeQuery[Long, EntityAttributeRecord, EntityAttributeTable](new EntityAttributeTable(_), EntityAttributeRecord)

  protected object workspaceAttributeQuery extends AttributeQuery[UUID, WorkspaceAttributeRecord, WorkspaceAttributeTable](new WorkspaceAttributeTable(_), WorkspaceAttributeRecord)

  protected object submissionAttributeQuery extends AttributeQuery[Long, SubmissionAttributeRecord, SubmissionAttributeTable](new SubmissionAttributeTable(_), SubmissionAttributeRecord)

  protected abstract class AttributeScratchQuery[OWNER_ID: TypeTag, RECORD <: AttributeRecord[OWNER_ID], TEMP_RECORD <: AttributeScratchRecord[OWNER_ID], T <: AttributeScratchTable[OWNER_ID, TEMP_RECORD]](cons: Tag => T, createRecord: (Long, OWNER_ID, String, String, Option[String], Option[Double], Option[Boolean], Option[Long], Option[Int], Option[Int], String) => TEMP_RECORD) extends TableQuery[T](cons) {
    def batchInsertAttributes(attributes: Seq[RECORD], transactionId: String) = {
      insertInBatches(this, attributes.map { case rec =>
        createRecord(rec.id, rec.ownerId, rec.namespace, rec.name, rec.valueString, rec.valueNumber, rec.valueBoolean, rec.valueEntityRef, rec.listIndex, rec.listLength, transactionId)
      })
    }
  }

  protected object entityAttributeScratchQuery extends AttributeScratchQuery[Long, EntityAttributeRecord, EntityAttributeScratchRecord, EntityAttributeScratchTable](new EntityAttributeScratchTable(_), EntityAttributeScratchRecord)
  protected object workspaceAttributeScratchQuery extends AttributeScratchQuery[UUID, WorkspaceAttributeRecord, WorkspaceAttributeScratchRecord, WorkspaceAttributeScratchTable](new WorkspaceAttributeScratchTable(_), WorkspaceAttributeScratchRecord)

  /**
   * @param createRecord function to create a RECORD object, parameters: id, ownerId, name, valueString, valueNumber, valueBoolean, None, listIndex, listLength
   * @tparam OWNER_ID the type of the ownerId field
   * @tparam RECORD the record class
   */
  protected abstract class AttributeQuery[OWNER_ID: TypeTag, RECORD <: AttributeRecord[OWNER_ID], T <: AttributeTable[OWNER_ID, RECORD]](cons: Tag => T, createRecord: (Long, OWNER_ID, String, String, Option[String], Option[Double], Option[Boolean], Option[Long], Option[Int], Option[Int]) => RECORD) extends TableQuery[T](cons)  {

    /**
     * Insert an attribute into the database. This will be multiple inserts for a list and will lookup
     * referenced entities.
     *
     * @param ownerId
     * @param attributeName
     * @param attribute
     * @param workspaceId used only for AttributeEntityReferences (or lists of them) to resolve the reference within the workspace
     * @return a sequence of write actions the resulting value being the attribute id inserted
     */
    def insertAttributeRecords(ownerId: OWNER_ID, attributeName: AttributeName, attribute: Attribute, workspaceId: UUID): Seq[ReadWriteAction[Int]] = {

      def insertEmptyVal: Seq[ReadWriteAction[Int]] = {
        Seq(insertAttributeValue(ownerId, attributeName, AttributeNumber(-1), Option(EMPTY_LIST_INDEX_MAGIC), Option(0)))
      }

      def insertEmptyRef: Seq[ReadWriteAction[Int]] = {
        Seq(this += marshalAttributeEmptyEntityReferenceList(ownerId, attributeName))
      }

      attribute match {
        case AttributeEntityReferenceEmptyList => insertEmptyRef
        case AttributeValueEmptyList => insertEmptyVal
        //convert empty AttributeList types to AttributeEmptyList on save
        case AttributeEntityReferenceList(refs) if refs.isEmpty => insertEmptyRef
        case AttributeValueList(values) if values.isEmpty => insertEmptyVal

        case AttributeEntityReferenceList(refs) =>
          assertConsistentReferenceListMembers(refs)
          refs.zipWithIndex.map { case (ref, index) => insertAttributeRef(ownerId, attributeName, workspaceId, ref, Option(index), Option(refs.length)) }
        case AttributeValueList(values) =>
          assertConsistentValueListMembers(values)
          values.zipWithIndex.map { case (value, index) => insertAttributeValue(ownerId, attributeName, value, Option(index), Option(values.length)) }
        case value: AttributeValue => Seq(insertAttributeValue(ownerId, attributeName, value))
        case ref: AttributeEntityReference => Seq(insertAttributeRef(ownerId, attributeName, workspaceId, ref))
      }
    }

    private def insertAttributeRef(ownerId: OWNER_ID, attributeName: AttributeName, workspaceId: UUID, ref: AttributeEntityReference, listIndex: Option[Int] = None, listLength: Option[Int] = None): ReadWriteAction[Int] = {
      uniqueResult[EntityRecord](entityQuery.findEntityByName(workspaceId, ref.entityType, ref.entityName)).flatMap {
        case None => throw new RawlsException(s"$ref not found in workspace $workspaceId")
        case Some(entityRecord) =>
          (this += marshalAttributeEntityReference(ownerId, attributeName, listIndex, ref, Map(ref -> entityRecord.id), listLength))
      }
    }

    private def insertAttributeValue(ownerId: OWNER_ID, attributeName: AttributeName, value: AttributeValue, listIndex: Option[Int] = None, listLength: Option[Int] = None): ReadWriteAction[Int] = {
      (this += marshalAttributeValue(ownerId, attributeName, value, listIndex, listLength))
    }

    def marshalAttribute(ownerId: OWNER_ID, attributeName: AttributeName, attribute: Attribute, entityIdsByRef: Map[AttributeEntityReference, Long]): Seq[T#TableElementType] = {

      def marshalEmptyVal : Seq[T#TableElementType] = {
        Seq(marshalAttributeValue(ownerId, attributeName, AttributeNumber(-1), Option(EMPTY_LIST_INDEX_MAGIC), Option(0)))
      }

      def marshalEmptyRef : Seq[T#TableElementType] = {
        Seq(marshalAttributeEmptyEntityReferenceList(ownerId, attributeName))
      }

      attribute match {
        case AttributeEntityReferenceEmptyList => marshalEmptyRef
        case AttributeValueEmptyList => marshalEmptyVal
        //convert empty AttributeList types to AttributeEmptyList on save
        case AttributeEntityReferenceList(refs) if refs.isEmpty => marshalEmptyRef
        case AttributeValueList(values) if values.isEmpty => marshalEmptyVal

        case AttributeEntityReferenceList(refs) =>
          assertConsistentReferenceListMembers(refs)
          refs.zipWithIndex.map { case (ref, index) => marshalAttributeEntityReference(ownerId, attributeName, Option(index), ref, entityIdsByRef, Option(refs.length))}

        case AttributeValueList(values) =>
          assertConsistentValueListMembers(values)
          values.zipWithIndex.map { case (value, index) => marshalAttributeValue(ownerId, attributeName, value, Option(index), Option(values.length))}
        case value: AttributeValue => Seq(marshalAttributeValue(ownerId, attributeName, value, None, None))
        case ref: AttributeEntityReference => Seq(marshalAttributeEntityReference(ownerId, attributeName, None, ref, entityIdsByRef, None))

      }
    }

    def batchInsertAttributes(attributes: Seq[RECORD]) = {
      insertInBatches(this, attributes)
    }

    def marshalAttributeEmptyEntityReferenceList(ownerId: OWNER_ID, attributeName: AttributeName): RECORD = {
      createRecord(0, ownerId, attributeName.namespace, attributeName.name, None, None, None, None, Option(EMPTY_LIST_INDEX_MAGIC), Option(0))
    }

    def marshalAttributeEntityReference(ownerId: OWNER_ID, attributeName: AttributeName, listIndex: Option[Int], ref: AttributeEntityReference, entityIdsByRef: Map[AttributeEntityReference, Long], listLength: Option[Int]): RECORD = {
      val entityId = entityIdsByRef.getOrElse(ref, throw new RawlsException(s"$ref not found"))
      createRecord(0, ownerId, attributeName.namespace, attributeName.name, None, None, None, Option(entityId), listIndex, listLength)
    }

    def marshalAttributeValue(ownerId: OWNER_ID, attributeName: AttributeName, value: AttributeValue, listIndex: Option[Int], listLength: Option[Int]): RECORD = {
      val valueBoolean = value match {
        case AttributeBoolean(b) => Option(b)
        case _ => None
      }
      val valueNumber = value match {
        case AttributeNumber(b) => Option(b.toDouble)
        case _ => None
      }
      val valueString = value match {
        case AttributeString(b) => Option(b)
        case _ => None
      }

      createRecord(0, ownerId, attributeName.namespace, attributeName.name, valueString, valueNumber, valueBoolean, None, listIndex, listLength)
    }

    def deleteAttributeRecords(attributeRecords: Seq[RECORD]): DBIOAction[Int, NoStream, Write] = {
      filter(_.id inSetBind attributeRecords.map(_.id)).delete
    }

    object AlterAttributesUsingScratchTableQueries extends RawSqlQuery {
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
      def deleteFromMasterAction(ownerIds: Seq[OWNER_ID], transactionId: String) =
        concatSqlActions(sql"""delete a from #${baseTableRow.tableName} a
                left join #${baseTableRow.tableName}_SCRATCH ta
                on ta.transaction_id = $transactionId and (a.namespace,a.name,a.owner_id,ifnull(a.list_index,-2))=(ta.namespace,ta.name,ta.owner_id,ifnull(ta.list_index,-2))
                where ta.owner_id is null and a.owner_id in """, ownerIdTail(ownerIds)).as[Int]
      def updateInMasterAction(ownerIds: Seq[OWNER_ID], transactionId: String) =
        sql"""update #${baseTableRow.tableName} a
                join #${baseTableRow.tableName}_SCRATCH ta
                on (a.namespace,a.name,a.owner_id,ifnull(a.list_index,-2))=(ta.namespace,ta.name,ta.owner_id,ifnull(ta.list_index,-2)) and ta.transaction_id = $transactionId
                set a.value_string=ta.value_string, a.value_number=ta.value_number, a.value_boolean=ta.value_boolean, a.value_entity_ref=ta.value_entity_ref, a.list_length=ta.list_length""".as[Int]
      def insertIntoMasterAction(ownerIds: Seq[OWNER_ID], transactionId: String) =
        concatSqlActions(sql"""insert into #${baseTableRow.tableName}(namespace,name,value_string,value_number,value_boolean,value_entity_ref,list_index,owner_id,list_length)
                select ta.namespace,ta.name,ta.value_string,ta.value_number,ta.value_boolean,ta.value_entity_ref,ta.list_index,ta.owner_id,ta.list_length
                from #${baseTableRow.tableName}_SCRATCH ta
                left join #${baseTableRow.tableName} a
                on (a.namespace,a.name,a.owner_id,a.list_index)<=>(ta.namespace,ta.name,ta.owner_id,ta.list_index) and a.owner_id in """, ownerIdTail(ownerIds),
                sql""" where ta.transaction_id = $transactionId and a.owner_id is null""").as[Int]

      def clearAttributeScratchTableAction(transactionId: String) = {
        sqlu"""delete from #${baseTableRow.tableName}_SCRATCH where transaction_id = $transactionId"""
      }

      def upsertAction(ownerIds: Seq[OWNER_ID], insertFunction: String => ReadWriteAction[Unit]) = {
        val transactionId = UUID.randomUUID().toString
        insertFunction(transactionId) andThen
          deleteFromMasterAction(ownerIds, transactionId) andThen
          updateInMasterAction(ownerIds, transactionId) andThen
          insertIntoMasterAction(ownerIds, transactionId) andFinally
          clearAttributeScratchTableAction(transactionId)
      }
    }

    def unmarshalAttributes[ID](allAttributeRecsWithRef: Seq[((ID, RECORD), Option[EntityRecord])]): Map[ID, AttributeMap] = {
      allAttributeRecsWithRef.groupBy { case ((id, attrRec), entOp) => id }.map { case (id, workspaceAttributeRecsWithRef) =>
        id -> workspaceAttributeRecsWithRef.groupBy { case ((id, attrRec), entOp) => AttributeName(attrRec.namespace, attrRec.name) }.map { case (attrName, attributeRecsWithRefForNameWithDupes) =>
          val attributeRecsWithRefForName = attributeRecsWithRefForNameWithDupes.map { case ((wsId, attributeRec), entityRec) => (attributeRec, entityRec) }.toSet
          val unmarshalled = if (attributeRecsWithRefForName.forall(_._1.listIndex.isDefined)) {
            unmarshalList(attributeRecsWithRefForName)
          } else if (attributeRecsWithRefForName.size > 1) {
            throw new RawlsException(s"more than one value exists for attribute but list index is not defined for all, records: $attributeRecsWithRefForName")
          } else if (attributeRecsWithRefForName.head._2.isDefined) {
            unmarshalReference(attributeRecsWithRefForName.head._2.get)
          } else {
            unmarshalValue(attributeRecsWithRefForName.head._1)
          }
          attrName -> unmarshalled
        }
      }
    }

    private def unmarshalList(attributeRecsWithRef: Set[(RECORD, Option[EntityRecord])]) = {
      val sortedRecs = attributeRecsWithRef.toSeq.sortBy(_._1.listIndex.get)
      //NOTE: listIndex of -1 means "empty list"
      if (sortedRecs.head._1.listIndex.get == EMPTY_LIST_INDEX_MAGIC) {
        if( isEntityRefRecord(sortedRecs.head._1) ) {
          AttributeEntityReferenceEmptyList
        } else {
          AttributeValueEmptyList
        }
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
  }
}
