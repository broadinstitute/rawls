package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.sql.Timestamp
import java.util.UUID

import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model._
import slick.ast.{BaseTypedType, TypedType}
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
  val valueJson: Option[String]
  val valueEntityRef: Option[Long]
  val listIndex: Option[Int]
  val listLength: Option[Int]
  val deleted: Boolean
  val deletedDate: Option[Timestamp]
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
                                 valueJson: Option[String],
                                 valueEntityRef: Option[Long],
                                 listIndex: Option[Int],
                                 listLength: Option[Int],
                                 deleted: Boolean,
                                 deletedDate: Option[Timestamp]) extends AttributeRecord[Long]

case class EntityAttributeScratchRecord(id: Long,
                                     ownerId: Long, // entity id
                                     namespace: String,
                                     name: String,
                                     valueString: Option[String],
                                     valueNumber: Option[Double],
                                     valueBoolean: Option[Boolean],
                                     valueJson: Option[String],
                                     valueEntityRef: Option[Long],
                                     listIndex: Option[Int],
                                     listLength: Option[Int],
                                     deleted: Boolean,
                                     deletedDate: Option[Timestamp],
                                     transactionId: String) extends AttributeScratchRecord[Long]

case class WorkspaceAttributeRecord(id: Long,
                                    ownerId: UUID, // workspace id
                                    namespace: String,
                                    name: String,
                                    valueString: Option[String],
                                    valueNumber: Option[Double],
                                    valueBoolean: Option[Boolean],
                                    valueJson: Option[String],
                                    valueEntityRef: Option[Long],
                                    listIndex: Option[Int],
                                    listLength: Option[Int],
                                    deleted: Boolean,
                                    deletedDate: Option[Timestamp]) extends AttributeRecord[UUID]

case class WorkspaceAttributeScratchRecord(id: Long,
                                        ownerId: UUID, // workspace id
                                        namespace: String,
                                        name: String,
                                        valueString: Option[String],
                                        valueNumber: Option[Double],
                                        valueBoolean: Option[Boolean],
                                        valueJson: Option[String],
                                        valueEntityRef: Option[Long],
                                        listIndex: Option[Int],
                                        listLength: Option[Int],
                                        deleted: Boolean,
                                        deletedDate: Option[Timestamp],
                                        transactionId: String) extends AttributeScratchRecord[UUID]

case class SubmissionAttributeRecord(id: Long,
                                     ownerId: Long, // validation id
                                     namespace: String,
                                     name: String,
                                     valueString: Option[String],
                                     valueNumber: Option[Double],
                                     valueBoolean: Option[Boolean],
                                     valueJson: Option[String],
                                     valueEntityRef: Option[Long],
                                     listIndex: Option[Int],
                                     listLength: Option[Int],
                                     deleted: Boolean,
                                     deletedDate: Option[Timestamp]) extends AttributeRecord[Long]

trait AttributeComponent {
  this: DriverComponent
    with EntityComponent
    with WorkspaceComponent
    with SubmissionComponent =>

  import driver.api._

  def isEntityRefRecord[T](rec: AttributeRecord[T]): Boolean = {
    val isEmptyRefListDummyRecord = rec.listLength.isDefined && rec.listLength.get == 0 && rec.valueNumber.isEmpty
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
    def valueJson = column[Option[String]]("value_json")
    def valueEntityRef = column[Option[Long]]("value_entity_ref")
    def listIndex = column[Option[Int]]("list_index")
    def listLength = column[Option[Int]]("list_length")
    def deleted = column[Boolean]("deleted")
    def deletedDate = column[Option[Timestamp]]("deleted_date")
  }

  abstract class AttributeScratchTable[OWNER_ID: TypedType, RECORD <: AttributeScratchRecord[OWNER_ID]](tag: Tag, tableName: String) extends AttributeTable[OWNER_ID, RECORD](tag, tableName) {
    def transactionId = column[String]("transaction_id")
  }

  class EntityAttributeTable(tag: Tag) extends AttributeTable[Long, EntityAttributeRecord](tag, "ENTITY_ATTRIBUTE") {
    def * = (id, ownerId, namespace, name, valueString, valueNumber, valueBoolean, valueJson, valueEntityRef, listIndex, listLength, deleted, deletedDate) <> (EntityAttributeRecord.tupled, EntityAttributeRecord.unapply)

    def uniqueIdx = index("UNQ_ENTITY_ATTRIBUTE", (ownerId, namespace, name, listIndex), unique = true)

    def entityRef = foreignKey("FK_ENT_ATTRIBUTE_ENTITY_REF", valueEntityRef, entityQuery)(_.id.?)
    def parentEntity = foreignKey("FK_ATTRIBUTE_PARENT_ENTITY", ownerId, entityQuery)(_.id)
  }

  class WorkspaceAttributeTable(tag: Tag) extends AttributeTable[UUID, WorkspaceAttributeRecord](tag, "WORKSPACE_ATTRIBUTE") {
    def * = (id, ownerId, namespace, name, valueString, valueNumber, valueBoolean, valueJson, valueEntityRef, listIndex, listLength, deleted, deletedDate) <> (WorkspaceAttributeRecord.tupled, WorkspaceAttributeRecord.unapply)

    def uniqueIdx = index("UNQ_WORKSPACE_ATTRIBUTE", (ownerId, namespace, name, listIndex), unique = true)

    def entityRef = foreignKey("FK_WS_ATTRIBUTE_ENTITY_REF", valueEntityRef, entityQuery)(_.id.?)
    def workspace = foreignKey("FK_ATTRIBUTE_PARENT_WORKSPACE", ownerId, workspaceQuery)(_.id)
  }

  class SubmissionAttributeTable(tag: Tag) extends AttributeTable[Long, SubmissionAttributeRecord](tag, "SUBMISSION_ATTRIBUTE") {
    def * = (id, ownerId, namespace, name, valueString, valueNumber, valueBoolean, valueJson, valueEntityRef, listIndex, listLength, deleted, deletedDate) <> (SubmissionAttributeRecord.tupled, SubmissionAttributeRecord.unapply)

    def uniqueIdx = index("UNQ_SUBMISSION_ATTRIBUTE", (ownerId, namespace, name, listIndex), unique = true)

    def entityRef = foreignKey("FK_SUB_ATTRIBUTE_ENTITY_REF", valueEntityRef, entityQuery)(_.id.?)
    def submissionValidation = foreignKey("FK_ATTRIBUTE_PARENT_SUB_VALIDATION", ownerId, submissionValidationQuery)(_.id)
  }

  class EntityAttributeScratchTable(tag: Tag) extends AttributeScratchTable[Long, EntityAttributeScratchRecord](tag, "ENTITY_ATTRIBUTE_SCRATCH") {
    def * = (id, ownerId, namespace, name, valueString, valueNumber, valueBoolean, valueJson, valueEntityRef, listIndex, listLength, deleted, deletedDate, transactionId) <> (EntityAttributeScratchRecord.tupled, EntityAttributeScratchRecord.unapply)
  }

  class WorkspaceAttributeScratchTable(tag: Tag) extends AttributeScratchTable[UUID, WorkspaceAttributeScratchRecord](tag, "WORKSPACE_ATTRIBUTE_SCRATCH") {
    def * = (id, ownerId, namespace, name, valueString, valueNumber, valueBoolean, valueJson, valueEntityRef, listIndex, listLength, deleted, deletedDate, transactionId) <>(WorkspaceAttributeScratchRecord.tupled, WorkspaceAttributeScratchRecord.unapply)
  }

  protected object entityAttributeQuery extends AttributeQuery[Long, EntityAttributeRecord, EntityAttributeTable](new EntityAttributeTable(_), EntityAttributeRecord)
  protected object workspaceAttributeQuery extends AttributeQuery[UUID, WorkspaceAttributeRecord, WorkspaceAttributeTable](new WorkspaceAttributeTable(_), WorkspaceAttributeRecord)
  protected object submissionAttributeQuery extends AttributeQuery[Long, SubmissionAttributeRecord, SubmissionAttributeTable](new SubmissionAttributeTable(_), SubmissionAttributeRecord)

  protected abstract class AttributeScratchQuery[OWNER_ID: TypeTag, RECORD <: AttributeRecord[OWNER_ID], TEMP_RECORD <: AttributeScratchRecord[OWNER_ID], T <: AttributeScratchTable[OWNER_ID, TEMP_RECORD]](cons: Tag => T, createRecord: (Long, OWNER_ID, String, String, Option[String], Option[Double], Option[Boolean], Option[String], Option[Long], Option[Int], Option[Int], Boolean, Option[Timestamp], String) => TEMP_RECORD) extends TableQuery[T](cons) {
    def insertScratchAttributes(attributeRecs: Seq[RECORD])(transactionId: String): WriteAction[Int] = {
      batchInsertAttributes(attributeRecs, transactionId)
    }

    def batchInsertAttributes(attributes: Seq[RECORD], transactionId: String) = {
      insertInBatches(this, attributes.map { case rec =>
        createRecord(rec.id, rec.ownerId, rec.namespace, rec.name, rec.valueString, rec.valueNumber, rec.valueBoolean, rec.valueJson, rec.valueEntityRef, rec.listIndex, rec.listLength, rec.deleted, rec.deletedDate, transactionId)
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
  protected abstract class AttributeQuery[OWNER_ID: TypeTag: BaseTypedType, RECORD <: AttributeRecord[OWNER_ID], T <: AttributeTable[OWNER_ID, RECORD]](cons: Tag => T, createRecord: (Long, OWNER_ID, String, String, Option[String], Option[Double], Option[Boolean], Option[String], Option[Long], Option[Int], Option[Int], Boolean, Option[Timestamp]) => RECORD) extends TableQuery[T](cons)  {

    def marshalAttribute(ownerId: OWNER_ID, attributeName: AttributeName, attribute: Attribute, entityIdsByRef: Map[AttributeEntityReference, Long]): Seq[T#TableElementType] = {

      def marshalEmptyVal : Seq[T#TableElementType] = {
        Seq(marshalAttributeValue(ownerId, attributeName, AttributeNumber(-1), None, Option(0)))
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

    private def marshalAttributeEmptyEntityReferenceList(ownerId: OWNER_ID, attributeName: AttributeName): RECORD = {
      createRecord(0, ownerId, attributeName.namespace, attributeName.name, None, None, None, None, None, None, Option(0), false, None)
    }

    private def marshalAttributeEntityReference(ownerId: OWNER_ID, attributeName: AttributeName, listIndex: Option[Int], ref: AttributeEntityReference, entityIdsByRef: Map[AttributeEntityReference, Long], listLength: Option[Int]): RECORD = {
      val entityId = entityIdsByRef.getOrElse(ref, throw new RawlsException(s"$ref not found"))
      createRecord(0, ownerId, attributeName.namespace, attributeName.name, None, None, None, None, Option(entityId), listIndex, listLength, false, None)
    }

    private def marshalAttributeValue(ownerId: OWNER_ID, attributeName: AttributeName, value: AttributeValue, listIndex: Option[Int], listLength: Option[Int]): RECORD = {
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

      val valueJson = value match {
        case AttributeValueRawJson(j) => Option(j.toString)
        case _ => None
      }

      createRecord(0, ownerId, attributeName.namespace, attributeName.name, valueString, valueNumber, valueBoolean, valueJson, None, listIndex, listLength, false, None)
    }

    def findByNameQuery(attrName: AttributeName) = {
      filter(rec => rec.namespace === attrName.namespace && rec.name === attrName.name)
    }

    def findByOwnerQuery(ownerIds: Seq[OWNER_ID]) = {
      filter(_.ownerId inSetBind ownerIds)
    }

    def queryByAttribute(attrName: AttributeName, attrValue: AttributeValue) = {
      findByNameQuery(attrName).filter { rec =>
        attrValue match {
          case AttributeString(s) => rec.valueString === s
          case AttributeNumber(n) => rec.valueNumber === n.doubleValue
          case AttributeBoolean(b) => rec.valueBoolean === b
          case _ => throw new RawlsException("Unsupported attribute type")
        }
      }
    }

    def findUniqueStringsByNameQuery(attrName: AttributeName, queryString: Option[String]) = {

      val basicFilter = filter(rec =>
        rec.namespace === attrName.namespace &&
          rec.name === attrName.name &&
          rec.valueString.isDefined)

      val res = (queryString match {
        case Some(query) => basicFilter.filter(_.valueString.like(s"%${query}%"))
        case None => basicFilter
      }).groupBy(_.valueString).map(queryThing =>
        (queryThing._1, queryThing._2.length))

      res.sortBy(r => (r._2.desc, r._1)).map(x => (x._1.get, x._2))
    }

    def deleteAttributeRecordsById(attributeRecordIds: Seq[Long]): DBIOAction[Int, NoStream, Write] = {
      filter(_.id inSetBind attributeRecordIds).delete
    }

    // for DB performance reasons, it's necessary to split "save attributes" into 3 steps:
    // - delete attributes which are not present in the new set
    // - insert attributes which are not present in the existing set
    // - update attributes which are common to both sets
    // Delete and Insert use standard Slick actions, but Update uses a scratch table
    // (AlterAttributesUsingScratchTableQueries) for DB performance reasons.

    // we use set operations (filter, contains, partition) to split the attributes,
    // which uses the element type's equals method.  Ideally we would use AttributeRecord.equals,
    // but we only want to use a subset of the fields: the primary key.
    // AttributeRecordPrimaryKey encapsulates only those fields.

    case class AttributeRecordPrimaryKey(ownerId: OWNER_ID, namespace: String, name: String, listIndex: Option[Int])

    // a map of PK -> Record allows us to perform set operations on the PKs but return the Records as results
    def toPrimaryKeyMap(recs: Traversable[RECORD]) =
      recs.map { rec => (AttributeRecordPrimaryKey(rec.ownerId, rec.namespace, rec.name, rec.listIndex), rec) }.toMap

    def patchAttributesAction(inserts: Traversable[RECORD], updates: Traversable[RECORD], deleteIds: Traversable[Long], insertFunction: Seq[RECORD] => String => WriteAction[Int]) = {
      deleteAttributeRecordsById(deleteIds.toSeq) andThen
        batchInsertAttributes(inserts.toSeq) andThen
        AlterAttributesUsingScratchTableQueries.updateAction(insertFunction(updates.toSeq))
    }

    def rewriteAttrsAction(attributesToSave: Traversable[RECORD], existingAttributes: Traversable[RECORD], insertFunction: Seq[RECORD] => String => WriteAction[Int]) = {
      val toSaveAttrMap = toPrimaryKeyMap(attributesToSave)
      val existingAttrMap = toPrimaryKeyMap(existingAttributes)

      // update attributes which are in both to-save and currently-exists, insert attributes which are in save but not exists
      val (attrsToUpdateMap, attrsToInsertMap) = toSaveAttrMap.partition { case (k, v) => existingAttrMap.keySet.contains(k) }
      // delete attributes which currently exist but are not in the attributes to save
      val attributesToDelete = existingAttrMap.filterKeys(! attrsToUpdateMap.keySet.contains(_)).values

      deleteAttributeRecordsById(attributesToDelete.map(_.id).toSeq) andThen
        batchInsertAttributes(attrsToInsertMap.values.toSeq) andThen
        AlterAttributesUsingScratchTableQueries.updateAction(insertFunction(attrsToUpdateMap.values.toSeq))
    }

    object AlterAttributesUsingScratchTableQueries extends RawSqlQuery {
      val driver: JdbcDriver = AttributeComponent.this.driver

      //MySQL seems to handle null safe operators inefficiently. the solution to this
      //is to use ifnull and default to -2 if the list_index is null. list_index of -2
      //is never used otherwise

      // updateInMasterAction: updates any row in *_ATTRIBUTE that also exists in *_ATTRIBUTE_SCRATCH
      def updateInMasterAction(transactionId: String) =
        sql"""update #${baseTableRow.tableName} a
                join #${baseTableRow.tableName}_SCRATCH ta
                on (a.namespace,a.name,a.owner_id,ifnull(a.list_index,-2))=(ta.namespace,ta.name,ta.owner_id,ifnull(ta.list_index,-2)) and ta.transaction_id = $transactionId
                set a.value_string=ta.value_string, a.value_number=ta.value_number, a.value_boolean=ta.value_boolean, a.value_json=ta.value_json, a.value_entity_ref=ta.value_entity_ref, a.list_length=ta.list_length, a.deleted=ta.deleted """.as[Int]

      def clearAttributeScratchTableAction(transactionId: String) = {
        sqlu"""delete from #${baseTableRow.tableName}_SCRATCH where transaction_id = $transactionId"""
      }

      def updateAction(insertIntoScratchFunction: String => WriteAction[Int]) = {
        val transactionId = UUID.randomUUID().toString
        insertIntoScratchFunction(transactionId) andThen
          updateInMasterAction(transactionId) andThen
          clearAttributeScratchTableAction(transactionId)
      }
    }

    def unmarshalAttributes[ID](allAttributeRecsWithRef: Seq[((ID, RECORD), Option[EntityRecord])]): Map[ID, AttributeMap] = {
      allAttributeRecsWithRef.groupBy { case ((id, attrRec), entOp) => id }.map { case (id, workspaceAttributeRecsWithRef) =>
        id -> workspaceAttributeRecsWithRef.groupBy { case ((_, attrRec), _) => AttributeName(attrRec.namespace, attrRec.name) }.map { case (attrName, attributeRecsWithRefForNameWithDupes) =>
          val attributeRecsWithRefForName = attributeRecsWithRefForNameWithDupes.map { case ((wsId, attributeRec), entityRec) => (attributeRec, entityRec) }.toSet
          val unmarshalled = if (attributeRecsWithRefForName.forall(_._1.listLength.isDefined)) {
            unmarshalList(attributeRecsWithRefForName)
          } else if (attributeRecsWithRefForName.size > 1) {
            throw new RawlsException(s"more than one value exists for attribute but list length not defined for all, records: $attributeRecsWithRefForName")
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
      val isEmptyList = attributeRecsWithRef.size == 1 && attributeRecsWithRef.head._1.listLength.getOrElse(-1) == 0

      val sortedRecs = try {
        attributeRecsWithRef.toSeq.sortBy(_._1.listIndex.get)
      } catch {
        case e: NoSuchElementException => throw new RawlsException(s"more than one value exists for attribute but list index not defined for all, records: $attributeRecsWithRef")
      }

      if (isEmptyList) {
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
        val n = attributeRec.valueNumber.get
        if (n.isValidInt) AttributeNumber(n.toInt) else AttributeNumber(n)
      } else if (attributeRec.valueString.isDefined) {
        AttributeString(attributeRec.valueString.get)
      } else if (attributeRec.valueJson.isDefined) {
        AttributeValueRawJson(attributeRec.valueJson.get)
      } else {
        AttributeNull
      }
    }

    private def unmarshalReference(referredEntity: EntityRecord): AttributeEntityReference = referredEntity.toReference
  }
}
