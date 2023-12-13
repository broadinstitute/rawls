package org.broadinstitute.dsde.rawls.dataaccess.slick

import akka.http.scaladsl.model.StatusCodes
import io.opencensus.scala.Tracing._
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.AttributeName.toDelimitedName
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.TracingUtils.traceDBIOWithParent
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport, RawlsFatalExceptionWithErrorReport}
import slick.ast.{BaseTypedType, TypedType}
import slick.dbio.Effect.Write
import slick.jdbc.JdbcProfile

import java.sql.Timestamp
import java.util.UUID
import scala.reflect.runtime.universe._

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
                                 deletedDate: Option[Timestamp]
) extends AttributeRecord[Long]

case class EntityAttributeTempRecord(id: Long,
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
                                     transactionId: String
) extends AttributeScratchRecord[Long]

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
                                    deletedDate: Option[Timestamp]
) extends AttributeRecord[UUID]

case class WorkspaceAttributeTempRecord(id: Long,
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
                                        transactionId: String
) extends AttributeScratchRecord[UUID]

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
                                     deletedDate: Option[Timestamp]
) extends AttributeRecord[Long]

trait AttributeComponent {
  this: DriverComponent with EntityComponent with WorkspaceComponent with SubmissionComponent =>

  import driver.api._

  type ShardId = String

  def isEntityRefRecord[T](rec: AttributeRecord[T]): Boolean = {
    val isEmptyRefListDummyRecord = rec.listLength.isDefined && rec.listLength.get == 0 && rec.valueNumber.isEmpty
    rec.valueEntityRef.isDefined || isEmptyRefListDummyRecord
  }

  abstract class AttributeTable[OWNER_ID: TypedType, RECORD <: AttributeRecord[OWNER_ID]](tag: Tag, tableName: String)
      extends Table[RECORD](tag, tableName) {
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

  abstract class AttributeScratchTable[OWNER_ID: TypedType, RECORD <: AttributeScratchRecord[OWNER_ID]](
    tag: Tag,
    tableName: String
  ) extends AttributeTable[OWNER_ID, RECORD](tag, tableName) {
    def transactionId = column[String]("transaction_id")
  }

  class EntityAttributeTable(shard: ShardId)(tag: Tag)
      extends AttributeTable[Long, EntityAttributeRecord](tag, s"ENTITY_ATTRIBUTE_$shard") {
    def * = (id,
             ownerId,
             namespace,
             name,
             valueString,
             valueNumber,
             valueBoolean,
             valueJson,
             valueEntityRef,
             listIndex,
             listLength,
             deleted,
             deletedDate
    ) <> (EntityAttributeRecord.tupled, EntityAttributeRecord.unapply)

    def uniqueIdx = index("UNQ_ENTITY_ATTRIBUTE", (ownerId, namespace, name, listIndex), unique = true)

    def entityRef = foreignKey(s"FK_ENT_ATTRIBUTE_ENTITY_REF_$shard", valueEntityRef, entityQuery)(_.id.?)
    def parentEntity = foreignKey(s"FK_ATTRIBUTE_PARENT_ENTITY_$shard", ownerId, entityQuery)(_.id)
  }

  class WorkspaceAttributeTable(tag: Tag)
      extends AttributeTable[UUID, WorkspaceAttributeRecord](tag, "WORKSPACE_ATTRIBUTE") {
    def * = (id,
             ownerId,
             namespace,
             name,
             valueString,
             valueNumber,
             valueBoolean,
             valueJson,
             valueEntityRef,
             listIndex,
             listLength,
             deleted,
             deletedDate
    ) <> (WorkspaceAttributeRecord.tupled, WorkspaceAttributeRecord.unapply)

    def uniqueIdx = index("UNQ_WORKSPACE_ATTRIBUTE", (ownerId, namespace, name, listIndex), unique = true)

    def entityRef = foreignKey("FK_WS_ATTRIBUTE_ENTITY_REF", valueEntityRef, entityQuery)(_.id.?)
    def workspace = foreignKey("FK_ATTRIBUTE_PARENT_WORKSPACE", ownerId, workspaceQuery)(_.id)
  }

  class SubmissionAttributeTable(tag: Tag)
      extends AttributeTable[Long, SubmissionAttributeRecord](tag, "SUBMISSION_ATTRIBUTE") {
    def * = (id,
             ownerId,
             namespace,
             name,
             valueString,
             valueNumber,
             valueBoolean,
             valueJson,
             valueEntityRef,
             listIndex,
             listLength,
             deleted,
             deletedDate
    ) <> (SubmissionAttributeRecord.tupled, SubmissionAttributeRecord.unapply)

    def uniqueIdx = index("UNQ_SUBMISSION_ATTRIBUTE", (ownerId, namespace, name, listIndex), unique = true)

    def entityRef = foreignKey("FK_SUB_ATTRIBUTE_ENTITY_REF", valueEntityRef, entityQuery)(_.id.?)
    def submissionValidation =
      foreignKey("FK_ATTRIBUTE_PARENT_SUB_VALIDATION", ownerId, submissionValidationQuery)(_.id)
  }

  class EntityAttributeTempTable(tag: Tag)
      extends AttributeScratchTable[Long, EntityAttributeTempRecord](tag, "ENTITY_ATTRIBUTE_TEMP") {
    def * = (id,
             ownerId,
             namespace,
             name,
             valueString,
             valueNumber,
             valueBoolean,
             valueJson,
             valueEntityRef,
             listIndex,
             listLength,
             deleted,
             deletedDate,
             transactionId
    ) <> (EntityAttributeTempRecord.tupled, EntityAttributeTempRecord.unapply)
  }

  class WorkspaceAttributeTempTable(tag: Tag)
      extends AttributeScratchTable[UUID, WorkspaceAttributeTempRecord](tag, "WORKSPACE_ATTRIBUTE_TEMP") {
    def * = (id,
             ownerId,
             namespace,
             name,
             valueString,
             valueNumber,
             valueBoolean,
             valueJson,
             valueEntityRef,
             listIndex,
             listLength,
             deleted,
             deletedDate,
             transactionId
    ) <> (WorkspaceAttributeTempRecord.tupled, WorkspaceAttributeTempRecord.unapply)
  }

  def determineShard(workspaceId: UUID): ShardId = {
    /* Sharded workspaces use a shard identifier such as "04_07", which translates into
        tables named ENTITY_ATTRIBUTE_04_07.
     */
    val shardSize = 4 // range of characters in a shard, e.g. 4 = "00-03"

    // see the liquibase changeset "20210923_sharded_entity_tables.xml" for expected shard identifier values
    val idString = workspaceId.toString.take(2).toCharArray
    val firstChar = idString(0) // first char of workspaceid
    val secondInt = Integer.parseInt(idString(1).toString, 16) // second character of workspaceId, as integer 0-15

    val lower = Math.floor(secondInt / shardSize) * shardSize
    val upper = lower + shardSize - 1

    s"$firstChar${lower.toLong.toHexString}_$firstChar${upper.toLong.toHexString}"
  }

  class EntityAttributeShardQuery(shard: ShardId)
      extends AttributeQuery[Long, EntityAttributeRecord, EntityAttributeTable](new EntityAttributeTable(shard)(_),
                                                                                EntityAttributeRecord
      )
  def entityAttributeShardQuery(workspaceId: UUID): EntityAttributeShardQuery =
    new EntityAttributeShardQuery(determineShard(workspaceId))
  def entityAttributeShardQuery(workspace: Workspace): EntityAttributeShardQuery =
    entityAttributeShardQuery(workspace.workspaceIdAsUUID)
  def entityAttributeShardQuery(entityRec: EntityRecord): EntityAttributeShardQuery =
    entityAttributeShardQuery(entityRec.workspaceId)

  protected object workspaceAttributeQuery
      extends AttributeQuery[UUID, WorkspaceAttributeRecord, WorkspaceAttributeTable](new WorkspaceAttributeTable(_),
                                                                                      WorkspaceAttributeRecord
      )
  protected object submissionAttributeQuery
      extends AttributeQuery[Long, SubmissionAttributeRecord, SubmissionAttributeTable](new SubmissionAttributeTable(_),
                                                                                        SubmissionAttributeRecord
      )

  // noinspection TypeAnnotation
  abstract protected class AttributeScratchQuery[OWNER_ID: TypeTag,
                                                 RECORD <: AttributeRecord[OWNER_ID],
                                                 TEMP_RECORD <: AttributeScratchRecord[OWNER_ID],
                                                 T <: AttributeScratchTable[OWNER_ID, TEMP_RECORD]
  ](cons: Tag => T,
    createRecord: (Long,
                   OWNER_ID,
                   String,
                   String,
                   Option[String],
                   Option[Double],
                   Option[Boolean],
                   Option[String],
                   Option[Long],
                   Option[Int],
                   Option[Int],
                   Boolean,
                   Option[Timestamp],
                   String
    ) => TEMP_RECORD
  ) extends TableQuery[T](cons) {
    def insertScratchAttributes(attributeRecs: Seq[RECORD])(transactionId: String): WriteAction[Int] =
      batchInsertAttributes(attributeRecs, transactionId)

    def batchInsertAttributes(attributes: Seq[RECORD], transactionId: String) =
      insertInBatches(
        this,
        attributes.map(rec =>
          createRecord(
            rec.id,
            rec.ownerId,
            rec.namespace,
            rec.name,
            rec.valueString,
            rec.valueNumber,
            rec.valueBoolean,
            rec.valueJson,
            rec.valueEntityRef,
            rec.listIndex,
            rec.listLength,
            rec.deleted,
            rec.deletedDate,
            transactionId
          )
        )
      )
  }

  protected object entityAttributeTempQuery
      extends AttributeScratchQuery[Long, EntityAttributeRecord, EntityAttributeTempRecord, EntityAttributeTempTable](
        new EntityAttributeTempTable(_),
        EntityAttributeTempRecord
      )
  protected object workspaceAttributeTempQuery
      extends AttributeScratchQuery[UUID,
                                    WorkspaceAttributeRecord,
                                    WorkspaceAttributeTempRecord,
                                    WorkspaceAttributeTempTable
      ](new WorkspaceAttributeTempTable(_), WorkspaceAttributeTempRecord)

  /**
   * @param createRecord function to create a RECORD object, parameters: id, ownerId, name, valueString, valueNumber, valueBoolean, None, listIndex, listLength
   * @tparam OWNER_ID the type of the ownerId field
   * @tparam RECORD the record class
   */
  // noinspection TypeAnnotation,EmptyCheck,ScalaUnusedSymbol
  abstract protected class AttributeQuery[OWNER_ID: TypeTag: BaseTypedType,
                                          RECORD <: AttributeRecord[OWNER_ID],
                                          T <: AttributeTable[OWNER_ID, RECORD]
  ](cons: Tag => T,
    createRecord: (Long,
                   OWNER_ID,
                   String,
                   String,
                   Option[String],
                   Option[Double],
                   Option[Boolean],
                   Option[String],
                   Option[Long],
                   Option[Int],
                   Option[Int],
                   Boolean,
                   Option[Timestamp]
    ) => RECORD
  ) extends TableQuery[T](cons) {

    def marshalAttribute(ownerId: OWNER_ID,
                         attributeName: AttributeName,
                         attribute: Attribute,
                         entityIdsByRef: Map[AttributeEntityReference, Long]
    ): Seq[T#TableElementType] = {

      def marshalEmptyVal: Seq[T#TableElementType] =
        Seq(marshalAttributeValue(ownerId, attributeName, AttributeNumber(-1), None, Option(0)))

      def marshalEmptyRef: Seq[T#TableElementType] =
        Seq(marshalAttributeEmptyEntityReferenceList(ownerId, attributeName))

      attribute match {
        case AttributeEntityReferenceEmptyList => marshalEmptyRef
        case AttributeValueEmptyList           => marshalEmptyVal
        // convert empty AttributeList types to AttributeEmptyList on save
        case AttributeEntityReferenceList(refs) if refs.isEmpty => marshalEmptyRef
        case AttributeValueList(values) if values.isEmpty       => marshalEmptyVal

        case AttributeEntityReferenceList(refs) =>
          assertConsistentReferenceListMembers(refs)
          refs.zipWithIndex.map { case (ref, index) =>
            marshalAttributeEntityReference(ownerId,
                                            attributeName,
                                            Option(index),
                                            ref,
                                            entityIdsByRef,
                                            Option(refs.length)
            )
          }

        case AttributeValueList(values) =>
          assertConsistentValueListMembers(values, attributeName)
          values.zipWithIndex.map { case (value, index) =>
            marshalAttributeValue(ownerId, attributeName, value, Option(index), Option(values.length))
          }
        case value: AttributeValue => Seq(marshalAttributeValue(ownerId, attributeName, value, None, None))
        case ref: AttributeEntityReference =>
          Seq(marshalAttributeEntityReference(ownerId, attributeName, None, ref, entityIdsByRef, None))

      }
    }

    def batchInsertAttributes(attributes: Seq[RECORD]) =
      insertInBatches(this, attributes)

    private def marshalAttributeEmptyEntityReferenceList(ownerId: OWNER_ID, attributeName: AttributeName): RECORD =
      createRecord(0,
                   ownerId,
                   attributeName.namespace,
                   attributeName.name,
                   None,
                   None,
                   None,
                   None,
                   None,
                   None,
                   Option(0),
                   false,
                   None
      )

    private def marshalAttributeEntityReference(ownerId: OWNER_ID,
                                                attributeName: AttributeName,
                                                listIndex: Option[Int],
                                                ref: AttributeEntityReference,
                                                entityIdsByRef: Map[AttributeEntityReference, Long],
                                                listLength: Option[Int]
    ): RECORD = {
      val entityId = entityIdsByRef.getOrElse(ref, throw new RawlsException(s"$ref not found"))
      createRecord(0,
                   ownerId,
                   attributeName.namespace,
                   attributeName.name,
                   None,
                   None,
                   None,
                   None,
                   Option(entityId),
                   listIndex,
                   listLength,
                   false,
                   None
      )
    }

    private def marshalAttributeValue(ownerId: OWNER_ID,
                                      attributeName: AttributeName,
                                      value: AttributeValue,
                                      listIndex: Option[Int],
                                      listLength: Option[Int]
    ): RECORD = {
      val valueBoolean = value match {
        case AttributeBoolean(b) => Option(b)
        case _                   => None
      }
      val valueNumber = value match {
        case AttributeNumber(b) => Option(b.toDouble)
        case _                  => None
      }
      val valueString = value match {
        case AttributeString(b) => Option(b)
        case _                  => None
      }

      val valueJson = value match {
        case AttributeValueRawJson(j) => Option(j.toString)
        case _                        => None
      }

      createRecord(0,
                   ownerId,
                   attributeName.namespace,
                   attributeName.name,
                   valueString,
                   valueNumber,
                   valueBoolean,
                   valueJson,
                   None,
                   listIndex,
                   listLength,
                   false,
                   None
      )
    }

    def findByNameQuery(attrName: AttributeName) =
      filter(rec => rec.namespace === attrName.namespace && rec.name === attrName.name)

    def findByOwnerQuery(ownerIds: Seq[OWNER_ID]) =
      filter(_.ownerId inSetBind ownerIds)

    def queryByAttribute(attrName: AttributeName, attrValue: AttributeValue) =
      findByNameQuery(attrName).filter { rec =>
        attrValue match {
          case AttributeString(s)  => rec.valueString === s
          case AttributeNumber(n)  => rec.valueNumber === n.doubleValue
          case AttributeBoolean(b) => rec.valueBoolean === b
          case _                   => throw new RawlsException("Unsupported attribute type")
        }
      }

    val caseSensitiveCollate = SimpleExpression.unary[Option[String], Option[String]] { (value, qb) =>
      qb.expr(value)
      qb.sqlBuilder += " collate utf8_bin"
    }

    /**
      * Returns all unique attribute values, with the specfied filters applied
      *
      * @param attrName the name of the attribute to filter for (note that this is a (namespace, name) pair)
      * @param queryString the string value to filter for, optional
      * @param limit the maximum number of results to return, optional
      * @param ownerIds the ownerIds (i.e. the entity ID or workspace ID that the attribute belongs to) to filter for, optional
      * @return the attribute values that meet the specified filters
      */
    def findUniqueStringsByNameQuery(attrName: AttributeName,
                                     queryString: Option[String],
                                     limit: Option[Int] = None,
                                     ownerIds: Option[Seq[OWNER_ID]] = None
    ) = {

      val basicFilter = filter { rec =>
        rec.namespace === attrName.namespace &&
        rec.name === attrName.name &&
        rec.valueString.isDefined
      }

      val withOptionalOwnerFilter =
        basicFilter.filterOpt(ownerIds) { case (table, ownerIds) =>
          table.ownerId inSetBind ownerIds
        }

      val baseQuery = (queryString match {
        case Some(query) => withOptionalOwnerFilter.filter(_.valueString.like(s"%$query%"))
        case None        => withOptionalOwnerFilter
      })
        .groupBy(r => caseSensitiveCollate(r.valueString))
        .map(queryThing => (queryThing._1, queryThing._2.length))
        .sortBy(r => (r._2.desc, r._1))

      val res = limit match {
        case Some(limit) => baseQuery.take(limit)
        case None        => baseQuery
      }

      res.map(x => (x._1.get, x._2))
    }

    def deleteAttributeRecordsById(attributeRecordIds: Seq[Long]): DBIOAction[Int, NoStream, Write] =
      filter(_.id inSetBind attributeRecordIds).delete

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
      recs.map(rec => (AttributeRecordPrimaryKey(rec.ownerId, rec.namespace, rec.name, rec.listIndex), rec)).toMap

    def patchAttributesAction(inserts: Traversable[RECORD],
                              updates: Traversable[RECORD],
                              deleteIds: Traversable[Long],
                              insertFunction: Seq[RECORD] => String => WriteAction[Int],
                              tracingContext: RawlsTracingContext
    ) =
      traceDBIOWithParent("patchAttributesAction", tracingContext) { span =>
        for {
          _ <-
            if (deleteIds.nonEmpty)
              traceDBIOWithParent("deleteAttributeRecordsById", span)(_ => deleteAttributeRecordsById(deleteIds.toSeq))
            else DBIO.successful(0)
          _ <-
            if (inserts.nonEmpty)
              traceDBIOWithParent("batchInsertAttributes", span)(_ => batchInsertAttributes(inserts.toSeq))
            else DBIO.successful(0)
          updateResult <-
            if (updates.nonEmpty)
              AlterAttributesUsingScratchTableQueries.updateAction(insertFunction(updates.toSeq), span)
            else
              DBIO.successful(0)
        } yield updateResult
      }

    def deleteAttributes(workspaceContext: Workspace, entityType: String, attributeNames: Set[AttributeName]) =
      workspaceQuery.updateLastModified(workspaceContext.workspaceIdAsUUID) andThen
        DeleteAttributeColumnQueries.deleteAttributeColumn(workspaceContext, entityType, attributeNames)

    def doesAttributeNameAlreadyExist(workspaceContext: Workspace,
                                      entityType: String,
                                      attributeName: AttributeName
    ): ReadAction[Option[Boolean]] =
      uniqueResult(AttributeColumnQueries.doesAttributeExist(workspaceContext, entityType, attributeName))

    def renameAttribute(workspaceContext: Workspace,
                        entityType: String,
                        oldAttributeName: AttributeName,
                        newAttributeName: AttributeName
    ): ReadWriteAction[Int] =
      for {
        numRowsRenamed <- AttributeColumnQueries.renameAttribute(workspaceContext,
                                                                 entityType,
                                                                 oldAttributeName,
                                                                 newAttributeName
        )
        _ <- workspaceQuery.updateLastModified(workspaceContext.workspaceIdAsUUID)
      } yield numRowsRenamed

    object DeleteAttributeColumnQueries extends RawSqlQuery {
      val driver: JdbcProfile = AttributeComponent.this.driver

      def deleteAttributeColumn(workspaceContext: Workspace, entityType: String, attributeNames: Set[AttributeName]) = {
        val attributeNamesSql = reduceSqlActionsWithDelim(attributeNames.map { attName =>
          sql"""(${attName.namespace},${attName.name})"""
        }.toSeq)

        val shardId = determineShard(workspaceContext.workspaceIdAsUUID)

        val deleteQueryBase = sql"""delete ea from ENTITY_ATTRIBUTE_#$shardId ea
                                    join ENTITY e on e.id = ea.owner_id
                                    where e.workspace_id = ${workspaceContext.workspaceIdAsUUID}
                                      and e.entity_type = ${entityType}
                                      and (ea.namespace, ea.name) in """

        concatSqlActions(deleteQueryBase, sql"(", attributeNamesSql, sql")").as[Int]
      }
    }

    object AttributeColumnQueries extends RawSqlQuery {
      val driver: JdbcProfile = AttributeComponent.this.driver

      def renameAttribute(workspaceContext: Workspace,
                          entityType: String,
                          oldAttributeName: AttributeName,
                          newAttributeName: AttributeName
      ): ReadWriteAction[Int] = {

        val shardId = determineShard(workspaceContext.workspaceIdAsUUID)

        sqlu"""update ENTITY_ATTRIBUTE_#$shardId ea join ENTITY e on ea.owner_id = e.id
             set ea.namespace=${newAttributeName.namespace}, ea.name=${newAttributeName.name}
             where e.workspace_id=${workspaceContext.workspaceIdAsUUID} and e.entity_type=$entityType
                and ea.namespace=${oldAttributeName.namespace} and ea.name=${oldAttributeName.name} and ea.deleted=0
        """
      }

      def doesAttributeExist(workspaceContext: Workspace,
                             entityType: String,
                             newAttributeName: AttributeName
      ): ReadAction[Seq[Boolean]] = {
        val shardId = determineShard(workspaceContext.workspaceIdAsUUID)

        sql"""select exists (select ea.name from ENTITY_ATTRIBUTE_#$shardId ea join ENTITY e on ea.owner_id = e.id
             where e.workspace_id=${workspaceContext.workspaceIdAsUUID} and e.entity_type=$entityType
                 and ea.namespace=${newAttributeName.namespace} and ea.name=${newAttributeName.name} and ea.deleted=0)
        """.as[Boolean]
      }
    }

    /**
     * Compares a collection of pre-existing attributes to a collection of attributes requested to save by a user,
     * finds the differences, saves the differences, and then returns the ids of the parent (entity|workspace) objects
     * that were affected.
     *
     * @param attributesToSave collection of attributes to be written
     * @param existingAttributes collection of pre-existing attributes to compare against
     * @param insertFunction function to use when writing attributes to the db (allows rewriteAttrsAction to be generic)
     * @return the ids of the parent (entity|workspace) objects that had attribute inserts/updates/deletes
     */
    def rewriteAttrsAction(attributesToSave: Traversable[RECORD],
                           existingAttributes: Traversable[RECORD],
                           insertFunction: Seq[RECORD] => String => WriteAction[Int]
    ): ReadWriteAction[Set[OWNER_ID]] = {
      val toSaveAttrMap = toPrimaryKeyMap(attributesToSave)
      val existingAttrMap = toPrimaryKeyMap(existingAttributes)

      // note that currently-existing attributes will have a populated id e.g. "1234", but to-save will have an id of "0"
      // therefore, we use this ComparableRecord class which omits the id when checking equality between existing and to-save.
      // note this does not include transactionId for AttributeScratchRecords. We do not expect AttributeScratchRecords
      // here, and transactionId will eventually be going away, so don't bother
      object ComparableRecord {
        def fromRecord(rec: RECORD): ComparableRecord =
          new ComparableRecord(
            rec.ownerId,
            rec.namespace,
            rec.name,
            rec.valueString,
            rec.valueNumber,
            rec.valueBoolean,
            rec.valueJson,
            rec.valueEntityRef,
            rec.listIndex,
            rec.listLength,
            rec.deleted,
            rec.deletedDate
          )
      }

      case class ComparableRecord(
        ownerId: OWNER_ID,
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
        deletedDate: Option[Timestamp]
      )

      // create a set of ComparableRecords representing the existing attributes
      val existingAttributesSet: Set[ComparableRecord] =
        existingAttributes.toSet.map(r => ComparableRecord.fromRecord(r))

      val existingKeys = existingAttrMap.keySet

      // insert attributes which are in save but not exists
      val attributesToInsert = toSaveAttrMap.filterKeys(!existingKeys.contains(_))

      // delete attributes which are in exists but not save
      val attributesToDelete = existingAttrMap.filterKeys(!toSaveAttrMap.keySet.contains(_))

      val attributesToUpdate = toSaveAttrMap.filter { case (k, v) =>
        existingKeys.contains(k) && // if the attribute doesn't already exist, don't attempt to update it
        !existingAttributesSet.contains(
          ComparableRecord.fromRecord(v)
        ) // if the attribute exists and is unchanged, don't update it
      }

      // collect the parent objects (e.g. entity, workspace) that have writes, so we know which object rows to re-calculate
      val ownersWithWrites: Set[OWNER_ID] = (attributesToInsert.values.map(_.ownerId) ++
        attributesToUpdate.values.map(_.ownerId) ++
        attributesToDelete.values.map(_.ownerId)).toSet

      // perform the inserts/updates/deletes
      patchAttributesAction(
        attributesToInsert.values,
        attributesToUpdate.values,
        attributesToDelete.values.map(_.id),
        insertFunction,
        RawlsTracingContext(Option(startSpan("rewriteAttrsAction")))
      )
        .map(_ => ownersWithWrites)
    }

    // noinspection SqlDialectInspection
    object AlterAttributesUsingScratchTableQueries extends RawSqlQuery {
      val driver: JdbcProfile = AttributeComponent.this.driver

      // MySQL seems to handle null safe operators inefficiently. the solution to this
      // is to use ifnull and default to 0 if the list_index is null.

      // Converting null to the 0 list_index enables converting single attributes to lists, and lists back to single
      // attributes.

      // updateInMasterAction: updates any row in *_ATTRIBUTE that also exists in *_ATTRIBUTE_SCRATCH
      def updateInMasterAction(transactionId: String) = {
        val joinTableName = getTempOrScratchTableName(baseTableRow.tableName)

        sql"""
          update #${baseTableRow.tableName} a
              join #${joinTableName} ta
              on (a.namespace, a.name, a.owner_id, ifnull(a.list_index, 0)) =
                 (ta.namespace, ta.name, ta.owner_id, ifnull(ta.list_index, 0))
                  and ta.transaction_id = $transactionId
          set a.value_string=ta.value_string,
              a.value_number=ta.value_number,
              a.value_boolean=ta.value_boolean,
              a.value_json=ta.value_json,
              a.value_entity_ref=ta.value_entity_ref,
              a.list_index=ta.list_index,
              a.list_length=ta.list_length,
              a.deleted=ta.deleted
             """.as[Int]
      }

      def clearAttributeScratchTableAction(transactionId: String) = {
        val joinTableName = getTempOrScratchTableName(baseTableRow.tableName)

        if (joinTableName.endsWith("SCRATCH"))
          sqlu"""delete from #${joinTableName} where transaction_id = $transactionId"""
        else DBIO.successful(0)
      }

      def updateAction(insertIntoScratchFunction: String => WriteAction[Int], tracingContext: RawlsTracingContext) =
        traceDBIOWithParent("updateAction", tracingContext) { span =>
          val transactionId = UUID.randomUUID().toString
          traceDBIOWithParent("insertIntoScratchFunction", span)(_ => insertIntoScratchFunction(transactionId)) andThen
            traceDBIOWithParent("updateInMasterAction", span)(_ => updateInMasterAction(transactionId)) andThen
            traceDBIOWithParent("clearAttributeScratchTableAction", span)(_ =>
              clearAttributeScratchTableAction(transactionId)
            )
        }
    }

    def unmarshalAttributes[ID](
      allAttributeRecsWithRef: Seq[((ID, RECORD), Option[EntityRecord])]
    ): Map[ID, AttributeMap] =
      allAttributeRecsWithRef.groupBy { case ((id, attrRec), entOp) => id }.map {
        case (id, workspaceAttributeRecsWithRef) =>
          id -> workspaceAttributeRecsWithRef
            .groupBy { case ((_, attrRec), _) => AttributeName(attrRec.namespace, attrRec.name) }
            .map { case (attrName, attributeRecsWithRefForNameWithDupes) =>
              val attributeRecsWithRefForName =
                attributeRecsWithRefForNameWithDupes.map { case ((wsId, attributeRec), entityRec) =>
                  (attributeRec, entityRec)
                }.toSet
              val unmarshalled = if (attributeRecsWithRefForName.forall(_._1.listLength.isDefined)) {
                unmarshalList(attributeRecsWithRefForName)
              } else if (attributeRecsWithRefForName.size > 1) {
                throw new RawlsException(
                  s"more than one value exists for attribute but list length not defined for all, records: $attributeRecsWithRefForName"
                )
              } else if (attributeRecsWithRefForName.head._2.isDefined) {
                unmarshalReference(attributeRecsWithRefForName.head._2.get)
              } else {
                unmarshalValue(attributeRecsWithRefForName.head._1)
              }
              attrName -> unmarshalled
            }
      }

    private def unmarshalList(attributeRecsWithRef: Set[(RECORD, Option[EntityRecord])]) = {
      val isEmptyList = attributeRecsWithRef.size == 1 && attributeRecsWithRef.head._1.listLength.getOrElse(-1) == 0

      val sortedRecs =
        try
          attributeRecsWithRef.toSeq.sortBy(_._1.listIndex.get)
        catch {
          case e: NoSuchElementException =>
            throw new RawlsException(
              s"more than one value exists for attribute but list index not defined for all, records: $attributeRecsWithRef"
            )
        }

      if (isEmptyList) {
        if (isEntityRefRecord(sortedRecs.head._1)) {
          AttributeEntityReferenceEmptyList
        } else {
          AttributeValueEmptyList
        }
      } else if (sortedRecs.head._2.isDefined) {
        AttributeEntityReferenceList(
          sortedRecs
            .map { case (attributeRec, entityRecOption) =>
              entityRecOption.getOrElse(
                throw new RawlsException(s"missing entity reference for attribute $attributeRec")
              )
            }
            .map(unmarshalReference)
        )
      } else {
        AttributeValueList(sortedRecs.map(_._1).map(unmarshalValue))
      }
    }

    private def assertConsistentValueListMembers(attributes: Seq[AttributeValue], attributeName: AttributeName): Unit =
      if (!attributes.isEmpty) {
        val headAttribute = attributes.head
        if (!attributes.forall(_.getClass == headAttribute.getClass)) {
          // determine the different attribute types
          val typeNames = attributes.map(_.getClass.getSimpleName).distinct
          // generate example values for those types
          val exampleValues = typeNames.flatMap { typeName =>
            attributes.find(x => x.getClass.getSimpleName == typeName).map(_.toString)
          }
          // generate a helpful error message
          val errMsg =
            s"inconsistent attributes for list: attribute lists must consist of a single data type. For attribute " +
              s"'${toDelimitedName(attributeName)}', found types: [${typeNames.mkString(", ")}]. " +
              s"Sample values for these types: [${exampleValues.map(_.take(100)).mkString(", ")}]"
          throw new RawlsFatalExceptionWithErrorReport(ErrorReport(errMsg))
        }
      }

    private def assertConsistentReferenceListMembers(attributes: Seq[AttributeEntityReference]): Unit =
      if (!attributes.isEmpty) {
        val headAttribute = attributes.head
        if (!attributes.forall(_.entityType == headAttribute.entityType)) {
          throw new RawlsExceptionWithErrorReport(
            ErrorReport(StatusCodes.BadRequest, s"inconsistent entity types for list: $attributes")
          )
        }
      }

    private def unmarshalValue(attributeRec: RECORD): AttributeValue =
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

    private def unmarshalReference(referredEntity: EntityRecord): AttributeEntityReference = referredEntity.toReference

    private def getTempOrScratchTableName(tableName: String): String =
      if (tableName.startsWith("ENTITY_ATTRIBUTE_")) {
        "ENTITY_ATTRIBUTE_TEMP"
      } else if (tableName == "WORKSPACE_ATTRIBUTE") {
        "WORKSPACE_ATTRIBUTE_TEMP"
      } else {
        s"${tableName}_SCRATCH"
      }

  }
}
