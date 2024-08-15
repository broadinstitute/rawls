package org.broadinstitute.dsde.rawls.dataaccess.slick

import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.LazyLogging
import io.opentelemetry.api.common.AttributeKey
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.AttributeName.toDelimitedName
import org.broadinstitute.dsde.rawls.model.{Workspace, _}
import org.broadinstitute.dsde.rawls.util.CollectionUtils
import org.broadinstitute.dsde.rawls.util.TracingUtils.{setTraceSpanAttribute, traceDBIOWithParent}
import org.broadinstitute.dsde.rawls.{
  model,
  RawlsException,
  RawlsExceptionWithErrorReport,
  RawlsFatalExceptionWithErrorReport
}
import slick.dbio.Effect.Read
import slick.jdbc.{GetResult, JdbcProfile, ResultSetConcurrency, ResultSetType, SQLActionBuilder, TransactionIsolation}
import slick.sql.SqlStreamingAction

import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.util.{Date, UUID}
import scala.annotation.tailrec
import scala.language.postfixOps

//noinspection TypeAnnotation
sealed trait EntityRecordBase {
  val id: Long
  val name: String
  val entityType: String
  val workspaceId: UUID
  val recordVersion: Long
  val deleted: Boolean
  val deletedDate: Option[Timestamp]

  def toReference = AttributeEntityReference(entityType, name)
  def withoutAllAttributeValues = EntityRecord(id, name, entityType, workspaceId, recordVersion, deleted, deletedDate)
  def withAllAttributeValues(allAttributeValues: Option[String]) = EntityRecordWithInlineAttributes(id,
                                                                                                    name,
                                                                                                    entityType,
                                                                                                    workspaceId,
                                                                                                    recordVersion,
                                                                                                    allAttributeValues,
                                                                                                    deleted,
                                                                                                    deletedDate
  )
}

case class EntityRecord(id: Long,
                        name: String,
                        entityType: String,
                        workspaceId: UUID,
                        recordVersion: Long,
                        deleted: Boolean,
                        deletedDate: Option[Timestamp]
) extends EntityRecordBase

case class EntityRecordWithInlineAttributes(id: Long,
                                            name: String,
                                            entityType: String,
                                            workspaceId: UUID,
                                            recordVersion: Long,
                                            allAttributeValues: Option[String],
                                            deleted: Boolean,
                                            deletedDate: Option[Timestamp]
) extends EntityRecordBase

// result structure from entity and attribute list raw sql
case class EntityAndAttributesResult(entityRecord: EntityRecord,
                                     attributeRecord: Option[EntityAttributeRecord],
                                     refEntityRecord: Option[EntityRecord]
)

object EntityComponent {
  // the length of the all_attribute_values column, which is TEXT, minus a few bytes because i'm nervous
  val allAttributeValuesColumnSize = 65532
}

import slick.jdbc.MySQLProfile.api._

sealed abstract class EntityTableBase[RECORD_TYPE <: EntityRecordBase](tag: Tag)
    extends Table[RECORD_TYPE](tag, "ENTITY") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
  def name = column[String]("name", O.Length(254))
  def entityType = column[String]("entity_type", O.Length(254))
  def workspaceId = column[UUID]("workspace_id")
  def version = column[Long]("record_version")
  def deleted = column[Boolean]("deleted")
  def deletedDate = column[Option[Timestamp]]("deleted_date")

//  def workspace = foreignKey("FK_ENTITY_WORKSPACE", workspaceId, workspaceQuery)(_.id)
  def uniqueTypeName = index("idx_entity_type_name", (workspaceId, entityType, name), unique = true)
}

class EntityTable(tag: Tag) extends EntityTableBase[EntityRecord](tag) {
  def * =
    (id, name, entityType, workspaceId, version, deleted, deletedDate) <> (EntityRecord.tupled, EntityRecord.unapply)
}

class EntityTableWithInlineAttributes(tag: Tag) extends EntityTableBase[EntityRecordWithInlineAttributes](tag) {
  def allAttributeValues = column[Option[String]]("all_attribute_values")

  def * = (id,
           name,
           entityType,
           workspaceId,
           version,
           allAttributeValues,
           deleted,
           deletedDate
  ) <> (EntityRecordWithInlineAttributes.tupled, EntityRecordWithInlineAttributes.unapply)
}

//noinspection TypeAnnotation
trait EntityComponent {
  this: DriverComponent
    with WorkspaceComponent
    with AttributeComponent
    with EntityTypeStatisticsComponent
    with EntityCacheComponent
    with EntityAttributeStatisticsComponent =>

  object entityQueryWithInlineAttributes extends TableQuery(new EntityTableWithInlineAttributes(_)) {
    type EntityQueryWithInlineAttributes = Query[EntityTableWithInlineAttributes, EntityRecordWithInlineAttributes, Seq]

    // only used by tests
    def findEntityByName(workspaceId: UUID, entityType: String, entityName: String): EntityQueryWithInlineAttributes =
      filter(entRec =>
        entRec.name === entityName && entRec.entityType === entityType && entRec.workspaceId === workspaceId
      )

    @tailrec
    def collectAttributeStrings(toProcess: Iterable[Attribute], processed: List[String], charsRemaining: Int): String =
      if (toProcess.isEmpty || charsRemaining <= 0)
        processed.mkString(" ")
      else {
        val nextAttr = AttributeStringifier(toProcess.head)
        collectAttributeStrings(toProcess.tail, processed :+ nextAttr, charsRemaining - nextAttr.length - 1)
      }

    private def createAllAttributesString(entity: Entity): Option[String] = {
      // maxLength is the max number of *bytes* we want to insert into the db
      val maxLength = EntityComponent.allAttributeValuesColumnSize
      // generate the all_attribute_values string. This has {maxLength} *characters* in it; if it contains multi-byte
      // characters then it will have > {maxLength} bytes in it
      val raw =
        s"${entity.name} ${collectAttributeStrings(entity.attributes.values.filterNot(_.isInstanceOf[AttributeList[_]]), List(), maxLength)}".toLowerCase
      // take the first {maxLength} bytes from the all_attribute_values string
      val bytes = raw.getBytes(StandardCharsets.UTF_8).take(maxLength)
      // convert the bytes back into a string. If the last character was a multi-byte character and we truncated it,
      // the last character will be corrupted, but we accept that possibility
      val limited = new String(bytes, StandardCharsets.UTF_8)

      Option(limited)
    }

    def batchInsertEntities(workspaceContext: Workspace,
                            entities: TraversableOnce[Entity]
    ): ReadWriteAction[Seq[EntityRecord]] = {
      def marshalNewEntity(entity: Entity, workspaceId: UUID): EntityRecordWithInlineAttributes =
        EntityRecordWithInlineAttributes(0,
                                         entity.name,
                                         entity.entityType,
                                         workspaceId,
                                         0,
                                         createAllAttributesString(entity),
                                         deleted = false,
                                         deletedDate = None
        )

      if (entities.nonEmpty) {
        val entityRecs = entities.toSeq.map(e => marshalNewEntity(e, workspaceContext.workspaceIdAsUUID))

        workspaceQuery.updateLastModified(workspaceContext.workspaceIdAsUUID) andThen
          DBIO
            .sequence(entityRecs.grouped(batchSize).map(entityQueryWithInlineAttributes ++= _))
            .map(_.flatten.sum)
            .andThen(
              entityQuery.getEntityRecords(workspaceContext.workspaceIdAsUUID, entityRecs.map(_.toReference).toSet)
            )
      } else {
        DBIO.successful(Seq.empty[EntityRecord])
      }
    }

    def insertNewEntities(workspaceContext: Workspace,
                          entities: Traversable[Entity],
                          existingEntityRefs: Seq[AttributeEntityReference]
    ): ReadWriteAction[Seq[EntityRecord]] = {
      val newEntities = entities.filterNot(e => existingEntityRefs.contains(e.toReference))
      // only insert the first instance of each entity, if the input contains duplicates.
      // without this distinct, we'll get DB errors because we're inserting the same
      // entityType + entityName combination twice.
      val insertableEntities = newEntities.iterator.distinctBy(_.toReference)

      batchInsertEntities(workspaceContext, insertableEntities)
    }

    def optimisticLockUpdate(entityRecs: Seq[EntityRecord],
                             entities: Traversable[Entity]
    ): ReadWriteAction[Seq[Int]] = {
      def populateAllAttributeValues(entityRecsFromDb: Seq[EntityRecord],
                                     entitiesToSave: Traversable[Entity]
      ): Seq[EntityRecordWithInlineAttributes] = {
        // "entitiesToSave" may contain duplicate entities. Ensure that we merge attributes for any duplicates
        // before grabbing their values.
        val entitiesByRef = entitiesToSave
          .foldLeft(Map.empty[AttributeEntityReference, Entity]) { (acc, e) =>
            val ref = e.toReference
            acc.get(ref) match {
              case None          => acc + ((ref, e))
              case Some(current) => acc.updated(ref, current.copy(attributes = current.attributes ++ e.attributes))
            }
          }
        entityRecsFromDb map { rec =>
          rec.withAllAttributeValues(createAllAttributesString(entitiesByRef(rec.toReference)))
        }
      }

      def findEntityByIdAndVersion(id: Long, version: Long): EntityQueryWithInlineAttributes =
        filter(rec => rec.id === id && rec.version === version)

      def optimisticLockUpdateOne(originalRec: EntityRecordWithInlineAttributes): ReadWriteAction[Int] =
        findEntityByIdAndVersion(originalRec.id, originalRec.recordVersion) update originalRec.copy(recordVersion =
          originalRec.recordVersion + 1
        ) map {
          case 0 =>
            throw new RawlsConcurrentModificationException(
              s"could not update $originalRec because its record version has changed"
            )
          case success => success
        }

      DBIO.sequence(populateAllAttributeValues(entityRecs, entities) map optimisticLockUpdateOne)
    }
  }

  object entityQuery extends TableQuery(new EntityTable(_)) with LazyLogging {

    type EntityQuery = Query[EntityTable, EntityRecord, Seq]
    type EntityAttributeQuery = Query[EntityAttributeTable, EntityAttributeRecord, Seq]

    // Raw queries - used when querying for multiple AttributeEntityReferences

    // noinspection SqlDialectInspection,DuplicatedCode
    private object EntityRecordRawSqlQuery extends RawSqlQuery {
      val driver: JdbcProfile = EntityComponent.this.driver
      implicit val getEntityRecord: GetResult[EntityRecord] =
        GetResult(r => EntityRecord(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

      def action(workspaceId: UUID, entities: Set[AttributeEntityReference]): ReadAction[Seq[EntityRecord]] =
        if (entities.isEmpty) {
          DBIO.successful(Seq.empty[EntityRecord])
        } else {
          val baseSelect =
            sql"select id, name, entity_type, workspace_id, record_version, deleted, deleted_date from ENTITY where workspace_id = $workspaceId and ("
          val entityTypeNameTuples = reduceSqlActionsWithDelim(
            entities.map(entity => sql"(entity_type = ${entity.entityType} and name = ${entity.entityName})").toSeq,
            sql" OR "
          )
          concatSqlActions(baseSelect, entityTypeNameTuples, sql")").as[EntityRecord]
        }

      def batchHide(workspaceId: UUID, entities: Seq[AttributeEntityReference]): ReadWriteAction[Seq[Int]] = {
        // get unique suffix for renaming
        val renameSuffix = "_" + getSufficientlyRandomSuffix(1000000000) // 1 billion
        val deletedDate = new Timestamp(new Date().getTime)

        // start of the SQL statement:
        val baseUpdateSql =
          sql"""update ENTITY set deleted=1, deleted_date=$deletedDate, name=CONCAT(name, $renameSuffix)
             where deleted=0 AND workspace_id=$workspaceId """

        // optimize for the common case where all entities being deleted have the same type
        val distinctTypes = entities.map(_.entityType).distinct

        val criteriaSql = if (distinctTypes.size == 1) {
          // and entity_type='mytype' and name in ('foo', 'bar', 'baz')
          val matchers = sql"""and entity_type=${distinctTypes.head} and name in ("""
          val entityTypeNameTuples = reduceSqlActionsWithDelim(entities.map(ref => sql"${ref.entityName}"))
          concatSqlActions(matchers, entityTypeNameTuples, sql")")
        } else {
          // and ( (entity_type='mytype1' and name='foo') or (entity_type='mytype2' and name='bar') or (entity_type='mytype3' and name='baz') )
          val matchers = sql"""and ("""
          val entityTypeNameTuples = reduceSqlActionsWithDelim(
            entities.map { ref =>
              sql"(entity_type = ${ref.entityType} and name = ${ref.entityName})"
            },
            sql" OR "
          )
          concatSqlActions(matchers, entityTypeNameTuples, sql")")
        }

        concatSqlActions(baseUpdateSql, criteriaSql).as[Int]
      }

      def batchHideType(workspaceId: UUID, entityType: String): ReadWriteAction[Seq[Int]] = {
        val renameSuffix = "_" + getSufficientlyRandomSuffix(1000000000) // 1 billion
        val deletedDate = new Timestamp(new Date().getTime)
        // issue bulk rename/hide for all entities of the specified type
        val typeUpdate =
          sql"""update ENTITY set deleted=1, deleted_date=$deletedDate, name=CONCAT(name, $renameSuffix) where deleted=0 AND workspace_id=$workspaceId and entity_type=$entityType"""
        typeUpdate.as[Int]
      }

      def activeActionForRefs(workspaceId: UUID,
                              entities: Set[AttributeEntityReference]
      ): ReadAction[Seq[AttributeEntityReference]] =
        if (entities.isEmpty) {
          DBIO.successful(Seq.empty[AttributeEntityReference])
        } else {
          val baseSelect = sql"select entity_type, name from ENTITY where workspace_id = $workspaceId and ("
          val entityTypeNameTuples = reduceSqlActionsWithDelim(
            entities.map(entity => sql"(entity_type = ${entity.entityType} and name = ${entity.entityName})").toSeq,
            sql" OR "
          )
          concatSqlActions(baseSelect, entityTypeNameTuples, sql") and deleted = 0").as[(String, String)].map { vect =>
            vect.map(pair => AttributeEntityReference(pair._1, pair._2))
          }
        }

    }

    // noinspection ScalaDocMissingParameterDescription,SqlDialectInspection,RedundantBlock,DuplicatedCode
    private object EntityAndAttributesRawSqlQuery extends RawSqlQuery {
      val driver: JdbcProfile = EntityComponent.this.driver

      // tells slick how to convert a result row from a raw sql query to an instance of EntityAndAttributesResult
      implicit val getEntityAndAttributesResult: GetResult[EntityAndAttributesResult] = GetResult { r =>
        // note that the number and order of all the r.<< match precisely with the select clause of baseEntityAndAttributeSql
        val entityRec = EntityRecord(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<)

        val attributeIdOption: Option[Long] = r.<<
        val attributeRecOption = attributeIdOption.map(id =>
          EntityAttributeRecord(id, entityRec.id, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<)
        )

        val refEntityRecOption = for {
          attributeRec <- attributeRecOption
          _ <- attributeRec.valueEntityRef
        } yield EntityRecord(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<)

        EntityAndAttributesResult(entityRec, attributeRecOption, refEntityRecOption)
      }

      // the where clause for this query is filled in specific to the use case
      def baseEntityAndAttributeSql(workspace: Workspace): String =
        baseEntityAndAttributeSql(
          workspace.workspaceIdAsUUID
        )

      private def baseEntityAndAttributeSql(workspaceId: UUID): String =
        baseEntityAndAttributeSql(determineShard(workspaceId))

      private def baseEntityAndAttributeSql(shardId: ShardId): String =
        s"""select e.id, e.name, e.entity_type, e.workspace_id, e.record_version, e.deleted, e.deleted_date,
          a.id, a.namespace, a.name, a.value_string, a.value_number, a.value_boolean, a.value_json, a.value_entity_ref, a.list_index, a.list_length, a.deleted, a.deleted_date,
          e_ref.id, e_ref.name, e_ref.entity_type, e_ref.workspace_id, e_ref.record_version, e_ref.deleted, e_ref.deleted_date
          from ENTITY e
          left outer join ENTITY_ATTRIBUTE_$shardId a on a.owner_id = e.id and a.deleted = e.deleted
          left outer join ENTITY e_ref on a.value_entity_ref = e_ref.id"""

      // Active actions: only return entities and attributes with their deleted flag set to false

      // almost the same as "activeActionForType" except 1) adds a sort by e.id; 2) returns a stream
      def activeStreamForType(workspaceContext: Workspace,
                              entityType: String
      ): SqlStreamingAction[Seq[EntityAndAttributesResult], EntityAndAttributesResult, Read] =
        sql"""#${baseEntityAndAttributeSql(workspaceContext)}
                where e.deleted = false
                and e.entity_type = ${entityType}
                and e.workspace_id = ${workspaceContext.workspaceIdAsUUID} order by e.id"""
          .as[EntityAndAttributesResult]

      def activeActionForRefs(workspaceContext: Workspace,
                              entityRefs: Set[AttributeEntityReference]
      ): ReadAction[Seq[EntityAndAttributesResult]] =
        if (entityRefs.isEmpty) {
          DBIO.successful(Seq.empty[EntityAndAttributesResult])
        } else {
          val baseSelect = sql"""#${baseEntityAndAttributeSql(
              workspaceContext
            )} where e.deleted = false and e.workspace_id = ${workspaceContext.workspaceIdAsUUID} and (e.entity_type, e.name) in ("""
          val entityTypeNameTuples = reduceSqlActionsWithDelim(entityRefs.map { ref =>
            sql"(${ref.entityType}, ${ref.entityName})"
          }.toSeq)
          concatSqlActions(baseSelect, entityTypeNameTuples, sql")").as[EntityAndAttributesResult]
        }

      def activeActionForWorkspace(workspaceContext: Workspace): ReadAction[Seq[EntityAndAttributesResult]] =
        sql"""#${baseEntityAndAttributeSql(
            workspaceContext
          )} where e.deleted = false and e.workspace_id = ${workspaceContext.workspaceIdAsUUID}"""
          .as[EntityAndAttributesResult]

      /**
        * Generates a sub query that can be filtered, sorted, sliced
        * @param workspaceId
        * @param entityType
        * @param sortFieldName
        * @return
        */
      private def paginationSubquery(workspaceId: UUID, entityType: String, sortFieldName: String) = {
        val shardId = determineShard(workspaceId)

        val (sortColumns, sortJoin) = sortFieldName match {
          case "name" =>
            (
              // no additional sort columns
              "",
              // no additional join required
              sql""
            )
          case _ =>
            // sortFieldName may be a namespace:name delimited string
            val sortAttr = AttributeName.fromDelimitedName(sortFieldName)
            (
              // select each attribute column and the referenced entity name
              """, sort_a.list_length as sort_list_length, sort_a.value_string as sort_field_string, sort_a.value_number as sort_field_number, sort_a.value_boolean as sort_field_boolean, sort_a.value_json as sort_field_json, sort_e_ref.name as sort_field_ref""",
              // join to attribute and entity (for references) table, grab only the named sort attribute and only the first element of a list
              sql"""left outer join ENTITY_ATTRIBUTE_#$shardId sort_a on sort_a.owner_id = e.id and sort_a.namespace = ${sortAttr.namespace} and sort_a.name = ${sortAttr.name} and ifnull(sort_a.list_index, 0) = 0 left outer join ENTITY sort_e_ref on sort_a.value_entity_ref = sort_e_ref.id """
            )
        }

        concatSqlActions(
          sql"""select e.id, e.name #$sortColumns from ENTITY e """,
          sortJoin,
          sql""" where e.deleted = 'false' and e.entity_type = $entityType and e.workspace_id = $workspaceId """
        )
      }

      // generate the clause to filter based on user search terms
      def paginationFilterSql(prefix: String, alias: String, entityQuery: model.EntityQuery) = {
        val filtersOption = entityQuery.filterTerms.map {
          _.split(" ").toSeq.map { term =>
            sql"concat(#$alias.name, ' ', #$alias.all_attribute_values) like ${'%' + term.toLowerCase + '%'}"
          }
        }

        filtersOption match {
          case None => sql""
          case Some(filters) =>
            concatSqlActions(
              sql"#$prefix (",
              reduceSqlActionsWithDelim(filters, sql" #${FilterOperators.toSql(entityQuery.filterOperator)} "),
              sql") "
            )
        }
      }

      def activeActionForMetadata(workspaceContext: Workspace,
                                  entityType: String,
                                  entityQuery: model.EntityQuery,
                                  parentContext: RawlsRequestContext
      ): ReadWriteAction[(Int, Int)] = {
        // standalone query to calculate the count of results that match our filter
        def filteredCountQuery: ReadAction[Vector[Int]] =
          entityQuery.columnFilter match {
            case Some(columnFilter) =>
              sql"""select count(distinct(e.id)) from ENTITY e, ENTITY_ATTRIBUTE_#${determineShard(
                  workspaceContext.workspaceIdAsUUID
                )} a
              where a.owner_id = e.id
              and e.deleted = 0
              and e.entity_type = $entityType
              and e.workspace_id = ${workspaceContext.workspaceIdAsUUID}
              and a.namespace = ${columnFilter.attributeName.namespace}
              and a.name = ${columnFilter.attributeName.name}
              and COALESCE(a.value_string, a.value_number) = ${columnFilter.term}
       """.as[Int]
            case _ =>
              val filteredQuery =
                sql"""select count(1) from ENTITY e
                where e.deleted = 0
                and e.entity_type = $entityType
                and e.workspace_id = ${workspaceContext.workspaceIdAsUUID} """
              concatSqlActions(filteredQuery, paginationFilterSql("and", "e", entityQuery)).as[Int]
          }

        for {
          unfilteredCount <- traceDBIOWithParent("findActiveEntityByType", parentContext)(_ =>
            findActiveEntityByType(workspaceContext.workspaceIdAsUUID, entityType).length.result
          )
          filteredCount <-
            if (entityQuery.filterTerms.isEmpty && entityQuery.columnFilter.isEmpty) {
              // if the query has no filter, then "filteredCount" and "unfilteredCount" will always be the same; no need to make another query
              DBIO.successful(Vector(unfilteredCount))
            } else {
              traceDBIOWithParent("filteredCountQuery", parentContext)(_ => filteredCountQuery)
            }

        } yield (unfilteredCount, filteredCount.head)
      }
      // END activeActionForMetadata

      def activeActionForEntityAndAttributesSource(workspaceContext: Workspace,
                                                   entityType: String,
                                                   entityQuery: model.EntityQuery,
                                                   parentContext: RawlsRequestContext
      ): SqlStreamingAction[Seq[EntityAndAttributesResult], EntityAndAttributesResult, Read] = {
        /*
          Lots of conditionals in here, to achieve the optimal SQL query for any given request. Pseudocode:

          When sorting by name, and requesting no attributes:
            select from ENTITY e order by e.name, with optional filter on e.all_attribute_values, limit, offset.

          All other requests:
            select from ENTITY e
              left outer join ENTITY_ATTRIBUTE to get entity values, restricting to just those requested by the user
              left outer join ENTITY to populate any attribute references
              join (subquery of select attribute-being-sorted-on from ENTITY
                      left join ENTITY_ATTRIBUTE
                      left join ENTITY
                      optional filter on e.all_attribute_values
                      sort by attribute, limit, offset) to get the proper pagination
         */

        // generate the clauses to limit the attributes returned to the user
        def attrSelectionSql(prefix: String) = {
          val fieldsOption = entityQuery.fields.fields.map { fieldList =>
            val attributeNameList: Set[AttributeName] = fieldList.map(AttributeName.fromDelimitedName)
            val attrClauses = attributeNameList.map(attrName =>
              sql"(a.namespace = ${attrName.namespace} AND a.name = ${attrName.name})"
            )
            concatSqlActions(sql"#$prefix (", reduceSqlActionsWithDelim(attrClauses.toSeq, sql" or "), sql") ")
          }

          fieldsOption.getOrElse(sql"")
        }

        // sorting clauses
        def order(alias: String) = {
          val prefix = if (alias == "") {
            ""
          } else {
            s"$alias."
          }
          entityQuery.sortField match {
            case "name" => sql" order by #${prefix}name #${SortDirections.toSql(entityQuery.sortDirection)} "
            case _ =>
              sql" order by #${prefix}sort_list_length #${SortDirections.toSql(entityQuery.sortDirection)}, #${prefix}sort_field_string #${SortDirections
                  .toSql(entityQuery.sortDirection)}, #${prefix}sort_field_number #${SortDirections.toSql(
                  entityQuery.sortDirection
                )}, #${prefix}sort_field_boolean #${SortDirections.toSql(entityQuery.sortDirection)}, #${prefix}sort_field_ref #${SortDirections
                  .toSql(entityQuery.sortDirection)}, #${prefix}name #${SortDirections.toSql(entityQuery.sortDirection)} "
          }
        }

        val filterByColumn =
          entityQuery.columnFilter match {
            case None =>
              setTraceSpanAttribute(parentContext, AttributeKey.booleanKey("isFilterByColumn"), java.lang.Boolean.FALSE)
              sql""
            case Some(columnFilter) =>
              setTraceSpanAttribute(parentContext, AttributeKey.booleanKey("isFilterByColumn"), java.lang.Boolean.TRUE)
              val shardId = determineShard(workspaceContext.workspaceIdAsUUID)

              sql""" and e.id in (
                select filter_e.id
                from ENTITY filter_e, ENTITY_ATTRIBUTE_#${shardId} filter_a
                where filter_a.owner_id = filter_e.id
                and filter_e.deleted = 0
                and filter_e.workspace_id = ${workspaceContext.workspaceIdAsUUID}
                and filter_e.entity_type = $entityType
                and filter_a.deleted = 0
                and filter_a.namespace = ${columnFilter.attributeName.namespace}
                and filter_a.name = ${columnFilter.attributeName.name}
                and COALESCE(filter_a.value_string, filter_a.value_number) = ${columnFilter.term}
             )"""
          }

        // additional joins-to-subquery to provide proper pagination
        val paginationJoin = concatSqlActions(
          sql""" join (""",
          paginationSubquery(workspaceContext.workspaceIdAsUUID, entityType, entityQuery.sortField),
          paginationFilterSql("and", "e", entityQuery),
          filterByColumn,
          order(""),
          sql" limit #${entityQuery.pageSize} offset #${(entityQuery.page - 1) * entityQuery.pageSize} ) p on p.id = e.id "
        )

        // the full query to generate the page of results, as requested by the user
        def pageQuery = {
          // did the user request any attributes other than "name" and "entityType"?
          val isRequestingAttrs: Boolean = entityQuery.fields.fields.forall { fieldSel =>
            (fieldSel diff Set("name", "entityType")).nonEmpty
          }

          val shardId = determineShard(workspaceContext.workspaceIdAsUUID)

          // different cases for which query to execute:
          if (entityQuery.sortField == "name" && !isRequestingAttrs) {
            // simplest; the user is sorting by ENTITY.name and doesn't want any attributes; we only need to
            // query the ENTITY table
            // NB: the "null" as the last column in the select is important! It allows this query to be translated into
            // EntityAndAttributesResult objects; the null represents the lack of any attributes for this entity
            concatSqlActions(
              sql"""select e.id, e.name, e.entity_type, e.workspace_id, e.record_version, e.deleted, e.deleted_date, null
                             from ENTITY e
                             where e.deleted = 'false' and e.entity_type = $entityType and e.workspace_id = ${workspaceContext.workspaceIdAsUUID} """,
              paginationFilterSql("and", "e", entityQuery),
              order("e"),
              sql" limit #${entityQuery.pageSize} offset #${(entityQuery.page - 1) * entityQuery.pageSize}"
            ).as[EntityAndAttributesResult]
          } else {
            // user is sorting by an attribute value, so we need the largest number of joins
            // this query is very similar to baseEntityAndAttributeSql
            /* TODO: include "where e.deleted = 'false' and e.entity_type = $entityType and e.workspace_id = $workspaceId"
                in the top-level select, to reduce what MySQL needs to look at? Does this actually help?
             */
            /* TODO: it's inefficient to return all columns of e_ref; we only really need the id and the name. We turn it into an
                EntityRecord, and then very quickly we read that EntityRecord's name and toss the rest of the info. Can we do better?
             */
            concatSqlActions(
              sql"""select e.id, e.name, e.entity_type, e.workspace_id, e.record_version, e.deleted, e.deleted_date,
                             a.id, a.namespace, a.name, a.value_string, a.value_number, a.value_boolean, a.value_json, a.value_entity_ref, a.list_index, a.list_length, a.deleted, a.deleted_date,
                             e_ref.id, e_ref.name, e_ref.entity_type, e_ref.workspace_id, e_ref.record_version, e_ref.deleted, e_ref.deleted_date
                             from ENTITY e
                             left outer join ENTITY_ATTRIBUTE_#$shardId a on a.owner_id = e.id and a.deleted = e.deleted """,
              attrSelectionSql(" and "),
              sql""" left outer join ENTITY e_ref on a.value_entity_ref = e_ref.id """,
              paginationJoin,
              order("p")
            ).as[EntityAndAttributesResult]
          }
        }

        pageQuery
      }
      // END activeActionForEntityAndAttributesSource

      // actions which may include "deleted" hidden entities

      def streamForTypeName(workspaceContext: Workspace,
                            entityType: String,
                            entityName: String,
                            desiredFields: Set[AttributeName]
      ): SqlStreamingAction[Seq[EntityAndAttributesResult], EntityAndAttributesResult, Read] = {
        // user requested specific attributes. include them in the where clause.
        val attrNamespaceNameTuples = reduceSqlActionsWithDelim(desiredFields.toSeq.map { attrName =>
          sql"(${attrName.namespace}, ${attrName.name})"
        })
        val attributesFilter =
          if (desiredFields.isEmpty) sql""
          else concatSqlActions(sql" and (a.namespace, a.name) in (", attrNamespaceNameTuples, sql")")

        concatSqlActions(
          sql"""#${baseEntityAndAttributeSql(
              workspaceContext
            )} where e.name = ${entityName} and e.entity_type = ${entityType} and e.workspace_id = ${workspaceContext.workspaceIdAsUUID}""",
          attributesFilter
        ).as[EntityAndAttributesResult]
      }

      def actionForIds(workspaceId: UUID, entityIds: Set[Long]): ReadAction[Seq[EntityAndAttributesResult]] =
        if (entityIds.isEmpty) {
          DBIO.successful(Seq.empty[EntityAndAttributesResult])
        } else {
          val baseSelect = sql"""#${baseEntityAndAttributeSql(workspaceId)} where e.id in ("""
          val entityIdSql = reduceSqlActionsWithDelim(entityIds.map(id => sql"$id").toSeq)
          concatSqlActions(baseSelect, entityIdSql, sql")").as[EntityAndAttributesResult]
        }

      def batchHide(workspaceContext: Workspace, entities: Seq[AttributeEntityReference]): ReadWriteAction[Seq[Int]] = {
        val shardId = determineShard(workspaceContext.workspaceIdAsUUID)
        // get unique suffix for renaming
        val renameSuffix = "_" + getSufficientlyRandomSuffix(1000000000) // 1 billion
        val deletedDate = new Timestamp(new Date().getTime)
        // issue bulk rename/hide for all entity attributes, given a set of entities
        val baseUpdate =
          sql"""update ENTITY_ATTRIBUTE_#$shardId ea join ENTITY e on ea.owner_id = e.id
                set ea.deleted=1, ea.deleted_date=$deletedDate, ea.name=CONCAT(ea.name, $renameSuffix)
                where e.workspace_id=${workspaceContext.workspaceIdAsUUID} and ea.deleted=0 and ("""
        val entityTypeNameTuples = reduceSqlActionsWithDelim(
          entities.map { ref =>
            sql"(e.entity_type = ${ref.entityType} and e.name = ${ref.entityName})"
          },
          sql" OR "
        )
        concatSqlActions(baseUpdate, entityTypeNameTuples, sql")").as[Int]
      }

      def batchHideAttributesOfType(workspaceContext: Workspace, entityType: String): ReadWriteAction[Seq[Int]] = {
        val shardId = determineShard(workspaceContext.workspaceIdAsUUID)
        // get unique suffix for renaming
        val renameSuffix = "_" + getSufficientlyRandomSuffix(1000000000) // 1 billion
        val deletedDate = new Timestamp(new Date().getTime)
        // issue bulk rename/hide for all entity attributes, given an entity type
        val baseUpdate =
          sql"""update ENTITY_ATTRIBUTE_#$shardId ea join ENTITY e on ea.owner_id = e.id
                set ea.deleted=1, ea.deleted_date=$deletedDate, ea.name=CONCAT(ea.name, $renameSuffix)
                where e.workspace_id=${workspaceContext.workspaceIdAsUUID} and ea.deleted=0 and e.entity_type=$entityType"""
        baseUpdate.as[Int]
      }

      def countReferencesToType(workspaceContext: Workspace, entityType: String): ReadAction[Vector[Int]] = {
        val shardId = determineShard(workspaceContext.workspaceIdAsUUID)

        val countQuery =
          sql"""select count(ea.id) from ENTITY doing_reference, ENTITY being_referenced, ENTITY_ATTRIBUTE_#$shardId ea where ea.value_entity_ref = being_referenced.id
               and ea.owner_id = doing_reference.id
               and being_referenced.entity_type=$entityType
               and doing_reference.entity_type!=$entityType
               and ea.deleted = 0
               and doing_reference.deleted = 0
               and being_referenced.workspace_id=${workspaceContext.workspaceIdAsUUID}"""

        countQuery.as[Int]
      }
    }

    // Raw query for performing actual deletion (not hiding) of everything that depends on an entity

    // noinspection SqlDialectInspection
    private object EntityDependenciesDeletionQuery extends RawSqlQuery {
      val driver: JdbcProfile = EntityComponent.this.driver

      def deleteAction(workspaceContext: Workspace): WriteAction[Int] = {
        val shardId = determineShard(workspaceContext.workspaceIdAsUUID)

        sqlu"""delete ea from ENTITY_ATTRIBUTE_#$shardId ea
               inner join ENTITY e
               on ea.owner_id = e.id
               where e.workspace_id=${workspaceContext.workspaceIdAsUUID}
          """
      }
    }

    // noinspection SqlDialectInspection
    private object CheckForExistingEntityTypeQuery extends RawSqlQuery {
      val driver: JdbcProfile = EntityComponent.this.driver

      def doesEntityTypeAlreadyExist(workspaceContext: Workspace, entityType: String): ReadAction[Seq[Boolean]] =
        sql"""select exists (select name from ENTITY
               where workspace_id=${workspaceContext.workspaceIdAsUUID} and entity_type = $entityType and deleted = 0)
          """.as[Boolean]
    }

    // noinspection SqlDialectInspection
    private object CopyEntitiesQuery extends RawSqlQuery {
      val driver: JdbcProfile = EntityComponent.this.driver

      def copyEntities(clonedWorkspaceId: UUID,
                       newWorkspaceId: UUID,
                       entityRefs: Set[AttributeEntityReference] = Set()
      ): WriteAction[Int] = {

        val baseInsert =
          sql"""insert into ENTITY (name, entity_type, workspace_id, record_version, all_attribute_values, deleted, deleted_date)
                select name, entity_type, $newWorkspaceId, record_version, all_attribute_values, 0, null from ENTITY e where workspace_id = $clonedWorkspaceId and deleted = 0
          """
        addEntitiesToWhereIfNeeded(entityRefs, baseInsert).head
      }

      def copyAttributes(clonedWorkspaceId: UUID,
                         newWorkspaceId: UUID,
                         entityRefs: Set[AttributeEntityReference] = Set()
      ): WriteAction[Int] = {

        val sourceShardId = determineShard(clonedWorkspaceId)
        val destShardId = determineShard(newWorkspaceId)
        val baseInsert = sql"""insert into ENTITY_ATTRIBUTE_#$destShardId (name, value_string, value_number, value_boolean, value_entity_ref,
                list_index, owner_id, list_length, namespace, VALUE_JSON, deleted, deleted_date)
                select ea.name, value_string, value_number, value_boolean, referenced_entity.new_id as value_entity_ref, list_index, owner_entity.new_id as owner_id,
                list_length, namespace, VALUE_JSON, 0, null from ENTITY_ATTRIBUTE_#$sourceShardId ea
                    -- join to mapping of source entity id to cloned entity id mapping table to update owner_id in ENTITY_ATTRIBUTE_shard
                    join (select old_entity.id as old_id, new_entity.id as new_id from ENTITY old_entity
                          join ENTITY new_entity on new_entity.entity_type = old_entity.entity_type
                          and new_entity.name = old_entity.name
                          and new_entity.workspace_id = $newWorkspaceId
                          and old_entity.workspace_id = $clonedWorkspaceId) owner_entity
                    on ea.owner_id = owner_entity.old_id
                    -- join to mapping of source entity id to cloned entity id mapping table to update value_entity_ref in ENTITY_ATTRIBUTE_shard
                    -- for entity reference attributes
                    left join (select old_entity.id as old_id, new_entity.id as new_id from ENTITY old_entity
                               join ENTITY new_entity on new_entity.entity_type = old_entity.entity_type
                               and new_entity.name = old_entity.name
                               and new_entity.workspace_id = $newWorkspaceId
                               and old_entity.workspace_id = $clonedWorkspaceId) referenced_entity
                    on ea.value_entity_ref = referenced_entity.old_id
                join ENTITY e on e.id = ea.owner_id where ea.deleted = false and e.deleted = false and e.workspace_id = $clonedWorkspaceId
          """
        addEntitiesToWhereIfNeeded(entityRefs, baseInsert).head
      }

      private def addEntitiesToWhereIfNeeded(entityRefs: Set[AttributeEntityReference], baseInsert: SQLActionBuilder) =
        if (entityRefs.nonEmpty) {
          val entityTypeNameTuples = reduceSqlActionsWithDelim(
            entityRefs.map { entity =>
              sql"(e.entity_type = ${entity.entityType} and e.name = ${entity.entityName})"
            }.toSeq,
            sql" OR "
          )
          concatSqlActions(baseInsert, sql" and (", entityTypeNameTuples, sql")").as[Int]
        } else {
          baseInsert.as[Int]
        }
    }

    // noinspection SqlDialectInspection
    private object ChangeEntityTypeNameQuery extends RawSqlQuery {
      val driver: JdbcProfile = EntityComponent.this.driver

      def changeEntityTypeName(workspaceContext: Workspace, oldName: String, newName: String): WriteAction[Int] =
        sqlu"""update ENTITY set entity_type = $newName
              where workspace_id=${workspaceContext.workspaceIdAsUUID} and entity_type = $oldName and deleted = 0
          """
    }

    /*
      These methods are only used by unit tests.
      They return full, materialized result sets without streaming, and these result sets can be
      quite large in real-world usage. Do not use those methods in user-facing runtime code.
     */
    object UnitTestHelpers extends RawSqlQuery {

      val driver: JdbcProfile = EntityComponent.this.driver

      def listActiveEntitiesOfType(workspaceContext: Workspace,
                                   entityType: String
      ): ReadAction[TraversableOnce[Entity]] =
        sql"""#${EntityAndAttributesRawSqlQuery.baseEntityAndAttributeSql(workspaceContext)}
        where e.deleted = false
        and e.entity_type = ${entityType}
        and e.workspace_id = ${workspaceContext.workspaceIdAsUUID}"""
          .as[EntityAndAttributesResult](EntityAndAttributesRawSqlQuery.getEntityAndAttributesResult)
          .map(query => unmarshalEntities(query))

      // includes "deleted" hidden entities
      def listEntities(workspaceContext: Workspace): ReadAction[TraversableOnce[Entity]] =
        sql"""#${EntityAndAttributesRawSqlQuery.baseEntityAndAttributeSql(
            workspaceContext
          )} where e.workspace_id = ${workspaceContext.workspaceIdAsUUID}"""
          .as[EntityAndAttributesResult](EntityAndAttributesRawSqlQuery.getEntityAndAttributesResult)
          .map(query => unmarshalEntities(query))

    }

    // Slick queries

    // Active queries: only return entities and attributes with their deleted flag set to false

    def findActiveEntityByType(workspaceId: UUID, entityType: String): EntityQuery =
      filter(entRec => entRec.entityType === entityType && entRec.workspaceId === workspaceId && !entRec.deleted)

    def findActiveEntityByWorkspace(workspaceId: UUID): EntityQuery =
      filter(entRec => entRec.workspaceId === workspaceId && !entRec.deleted)

    /**
      * given a set of AttributeEntityReference, query the db and return those refs that
      * are 1) active and 2) exist in the specified workspace. Use this method to validate user input
      * with the lightest SQL query; it does not fetch attributes or extraneous columns
      *
      * @param workspaceId the workspace to query for entity refs
      * @param entities the refs for which to query
      * @return the subset of refs that are active and found in the workspace
      */
    def getActiveRefs(workspaceId: UUID,
                      entities: Set[AttributeEntityReference]
    ): ReadAction[Seq[AttributeEntityReference]] =
      EntityRecordRawSqlQuery.activeActionForRefs(workspaceId, entities)

    private def findActiveAttributesByEntityId(workspaceId: UUID, entityId: Rep[Long]): EntityAttributeQuery = for {
      entityAttrRec <- entityAttributeShardQuery(workspaceId)
      if entityAttrRec.ownerId === entityId && !entityAttrRec.deleted
    } yield entityAttrRec

    // queries which may include "deleted" hidden entities

    def findEntityByName(workspaceId: UUID, entityType: String, entityName: String): EntityQuery =
      filter(entRec =>
        entRec.name === entityName && entRec.entityType === entityType && entRec.workspaceId === workspaceId
      )

    def findEntityById(id: Long): EntityQuery =
      filter(_.id === id)

    // Actions

    // get a specific entity or set of entities: may include "hidden" deleted entities if not named "active"
    def get(workspaceContext: Workspace,
            entityType: String,
            entityName: String,
            desiredFields: Set[AttributeName] = Set.empty
    ): ReadAction[Option[Entity]] =
      EntityAndAttributesRawSqlQuery.streamForTypeName(workspaceContext, entityType, entityName, desiredFields) map (
        query => unmarshalEntities(query)
      ) map (_.headOption)

    def getEntities(workspaceId: UUID, entityIds: Traversable[Long]): ReadAction[Seq[(Long, Entity)]] =
      EntityAndAttributesRawSqlQuery.actionForIds(workspaceId, entityIds.toSet) map (query =>
        unmarshalEntitiesWithIds(query)
      )

    def getEntityRecords(workspaceId: UUID, entities: Set[AttributeEntityReference]): ReadAction[Seq[EntityRecord]] = {
      val entitiesGrouped = entities.grouped(batchSize).toSeq

      DBIO
        .sequence(entitiesGrouped map { batch =>
          EntityRecordRawSqlQuery.action(workspaceId, batch)
        })
        .map(_.flatten)
    }

    def getActiveEntities(workspaceContext: Workspace,
                          entityRefs: Traversable[AttributeEntityReference]
    ): ReadAction[TraversableOnce[Entity]] =
      EntityAndAttributesRawSqlQuery.activeActionForRefs(workspaceContext, entityRefs.toSet) map (query =>
        unmarshalEntities(query)
      )

    // list all entities or those in a category

    def listActiveEntities(workspaceContext: Workspace): ReadAction[TraversableOnce[Entity]] =
      EntityAndAttributesRawSqlQuery.activeActionForWorkspace(workspaceContext) map (query => unmarshalEntities(query))

    // almost the same as "listActiveEntitiesOfType" except 1) does not unmarshal entities; 2) returns a stream
    def streamActiveEntityAttributesOfType(workspaceContext: Workspace,
                                           entityType: String
    ): SqlStreamingAction[Seq[EntityAndAttributesResult], EntityAndAttributesResult, Read] =
      EntityAndAttributesRawSqlQuery.activeStreamForType(workspaceContext, entityType)

    def getEntityTypesWithCounts(workspaceId: UUID): ReadAction[Map[String, Int]] =
      findActiveEntityByWorkspace(workspaceId)
        .groupBy(e => e.entityType)
        .map { case (entityType, entities) =>
          (entityType, entities.length)
        }
        .result map { result =>
        result.toMap
      }

    /**
      * Find the distinct attribute names associated with each entity type in the workspace.
      * @param workspaceId the workspace to query
      * @param queryTimeout the current query timeout limit in seconds; zero means there is
      *                     no limit
      * @return result set containing entity types -> seq of attribute names
      */
    def getAttrNamesAndEntityTypes(workspaceId: UUID,
                                   queryTimeout: Int = 0
    ): ReadAction[Map[String, Seq[AttributeName]]] = {
      val typesAndAttrNames = for {
        entityRec <- findActiveEntityByWorkspace(workspaceId)
        attrib <- findActiveAttributesByEntityId(workspaceId, entityRec.id)
      } yield (entityRec.entityType, (attrib.namespace, attrib.name))

      typesAndAttrNames.distinct.result.withStatementParameters(statementInit = _.setQueryTimeout(queryTimeout)) map {
        result =>
          CollectionUtils.groupByTuples(result.map { case (entityType: String, (ns: String, n: String)) =>
            (entityType, AttributeName(ns, n))
          })
      }
    }

    def loadSingleEntityForPage(workspaceContext: Workspace,
                                entityType: String,
                                entityName: String,
                                entityQuery: model.EntityQuery
    ): ReadWriteAction[(Int, Int, Iterable[Entity])] =
      for {
        unfilteredCount <- findActiveEntityByType(workspaceContext.workspaceIdAsUUID, entityType).length.result

        desiredFields = entityQuery.fields.fields.getOrElse(Set.empty).map(AttributeName.fromDelimitedName)
        optEntity <- get(workspaceContext, entityType, entityName, desiredFields)
      } yield
        if (optEntity.isEmpty) {
          // if we didn't find an entity of this name, nothing else to do
          (unfilteredCount, 0, Seq.empty)
        } else {
          val page = optEntity.toSeq
          (unfilteredCount, page.size, page)
        }

    def loadEntityPageCounts(workspaceContext: Workspace,
                             entityType: String,
                             entityQuery: model.EntityQuery,
                             parentContext: RawlsRequestContext
    ): ReadWriteAction[(Int, Int)] =
      EntityAndAttributesRawSqlQuery.activeActionForMetadata(workspaceContext, entityType, entityQuery, parentContext)

    /**
      * Returns a streaming result set of EntityAndAttributesResult objects, representing the individual attributes
      * within a set of Entities. Respects pagination, filtering, sorting, and other features of EntityQuery.
      *
      * @param workspaceContext the workspace containing the entities to be queried
      * @param entityType the type of entities to be queried
      * @param entityQuery criteria for querying entities
      * @param parentContext a tracing context under which this method should add its own traces
      * @return the result set, configured for streaming
      */
    def loadEntityPageSource(workspaceContext: Workspace,
                             entityType: String,
                             entityQuery: model.EntityQuery,
                             parentContext: RawlsRequestContext
    ): SqlStreamingAction[Seq[EntityAndAttributesResult], EntityAndAttributesResult, Read] = {
      // look for a columnFilter that specifies the primary key for this entityType;
      // such a columnFilter means we are filtering by name and can greatly simplify the underlying query.
      val nameFilter: Option[String] = entityQuery.columnFilter match {
        case Some(colFilter)
            if colFilter.attributeName == AttributeName.withDefaultNS(
              entityType + Attributable.entityIdAttributeSuffix
            ) =>
          Option(colFilter.term)
        case _ => None
      }

      // if filtering by name, retrieve that entity directly, else do the full query:
      setTraceSpanAttribute(parentContext,
                            AttributeKey.booleanKey("isFilterByName"),
                            java.lang.Boolean.valueOf(nameFilter.isDefined)
      )
      nameFilter match {
        case Some(entityName) =>
          val desiredFields = entityQuery.fields.fields.getOrElse(Set.empty).map(AttributeName.fromDelimitedName)
          EntityAndAttributesRawSqlQuery
            .streamForTypeName(workspaceContext, entityType, entityName, desiredFields)
        case _ =>
          EntityAndAttributesRawSqlQuery
            .activeActionForEntityAndAttributesSource(workspaceContext, entityType, entityQuery, parentContext)
      }
    }
    // END loadEntityPageSource

    // create or replace entities

    // TODO: can this be optimized? It nicely reuses the save(..., entities) method, but that method
    // does a lot of work. This single-entity save could, for instance, look for simple cases e.g. no references,
    // and take an easier code path.
    def save(workspaceContext: Workspace, entity: Entity): ReadWriteAction[Entity] =
      save(workspaceContext, Seq(entity)).map(_.head)

    def save(workspaceContext: Workspace, entities: Traversable[Entity]): ReadWriteAction[Traversable[Entity]] = {
      entities.foreach(validateEntity)

      for {
        _ <- workspaceQuery.updateLastModified(workspaceContext.workspaceIdAsUUID)
        preExistingEntityRecs <- getEntityRecords(workspaceContext.workspaceIdAsUUID, entities.map(_.toReference).toSet)
        savingEntityRecs <- entityQueryWithInlineAttributes
          .insertNewEntities(workspaceContext, entities, preExistingEntityRecs.map(_.toReference))
          .map(_ ++ preExistingEntityRecs)
        referencedAndSavingEntityRecs <- lookupNotYetLoadedReferences(workspaceContext,
                                                                      entities,
                                                                      savingEntityRecs.map(_.toReference)
        ).map(_ ++ savingEntityRecs)

        actuallyUpdatedEntityIds <- rewriteAttributes(
          workspaceContext.workspaceIdAsUUID,
          entities,
          savingEntityRecs.map(_.id),
          referencedAndSavingEntityRecs.map(e => e.toReference -> e.id).toMap
        )
        // find the pre-existing records that we updated
        actuallyUpdatedPreExistingEntityRecs = preExistingEntityRecs.filter(e =>
          actuallyUpdatedEntityIds.contains(e.id)
        )

        // find any entities that were repeated in the input payload. These repeated entities
        // may translate to one insert and one update; we need to make sure the extra updates
        // properly calculate all_attribute_values in the call to optimisticLockUpdate below.
        repeats = entities.groupBy(_.toReference).filter(_._2.size > 1).keySet
        // narrow the repeats to only those that triggered an insert (as opposed to repeats
        // being multiple updates)
        insertedRepeats = savingEntityRecs.filter(e => repeats.contains(e.toReference))

        // the records that need a call to optimisticLockUpdate are those that:
        //  1) pre-existed and were updated
        //  2) were repeated in the input payload, causing one insert and subsequent update(s)
        recsToUpdate = (actuallyUpdatedPreExistingEntityRecs ++ insertedRepeats).distinct

        _ <- entityQueryWithInlineAttributes.optimisticLockUpdate(recsToUpdate, entities)
      } yield entities
    }

    private def applyEntityPatch(workspaceContext: Workspace,
                                 entityRecord: EntityRecord,
                                 upserts: AttributeMap,
                                 deletes: Traversable[AttributeName],
                                 tracingContext: RawlsTracingContext
    ) = {
      traceDBIOWithParent("applyEntityPatch", tracingContext) { span =>
        // yank the attribute list for this entity to determine what to do with upserts
        traceDBIOWithParent("findByOwnerQuery", span)(_ =>
          entityAttributeShardQuery(workspaceContext)
            .findByOwnerQuery(Seq(entityRecord.id))
            .map(attr => (attr.namespace, attr.name, attr.id))
            .result
        ) flatMap { attrCols =>
          val existingAttrsToRecordIds: Map[AttributeName, Set[Long]] =
            attrCols
              .groupBy { case (namespace, name, _) =>
                (namespace, name)
              }
              .map { case ((namespace, name), ids) =>
                AttributeName(namespace, name) -> ids
                  .map(_._3)
                  .toSet // maintain the full list of attribute ids since list members are stored as individual attributes
              }

          val entityRefsToLookup = upserts.valuesIterator.collect { case e: AttributeEntityReference => e }.toSet

          /*
          Additional check for resizing entities whose Attribute value type is AttributeList[_] in response to bugs
          mentioned in WA-32 and WA-153. Currently the update query is such that it matches the listIndex of each
          attribute value and updates the list index and list size. Previously if the size of the list changes, those
          changes were not reflected in the table resulting in the behavior mentioned in the WA-32.

          Hence, for any attribute in the update list with value type as AttributeList[_], check if the size of the
          list has changed. If yes, based on increase or decrease of the list size, add those extra records into insertRecs
          or delete extra records from the entity table respectively.

          NOTE: Attributes that are not lists are treated as a list of size one.
           */

          // Tuple of
          // insertRecs: Seq[EntityAttributeRecord]
          // updateRecs: Seq[EntityAttributeRecord]
          // extraDeleteIds: Seq[Long]
          type AttributeModifications = (Seq[EntityAttributeRecord], Seq[EntityAttributeRecord], Seq[Long])

          def checkAndUpdateRecSize(name: AttributeName,
                                    existingAttrSize: Int,
                                    updateAttrSize: Int,
                                    attrRecords: Seq[EntityAttributeRecord]
          ): AttributeModifications =
            if (updateAttrSize > existingAttrSize) {
              // since the size of the "list" has increased, move these new records to insertRecs
              val (newInsertRecs, newUpdateRecs) = attrRecords.partition {
                _.listIndex.getOrElse(0) > (existingAttrSize - 1)
              }

              (newInsertRecs, newUpdateRecs, Seq.empty[Long])
            } else if (updateAttrSize < existingAttrSize) {
              // since the size of the list has decreased, delete the extra rows from table
              val deleteIds = existingAttrsToRecordIds(name).toSeq.takeRight(existingAttrSize - updateAttrSize)
              (Seq.empty[EntityAttributeRecord], attrRecords, deleteIds)
            } else (Seq.empty[EntityAttributeRecord], attrRecords, Seq.empty[Long]) // the list size hasn't changed

          def recordsForUpdateAttribute(name: AttributeName,
                                        attribute: Attribute,
                                        attrRecords: Seq[EntityAttributeRecord]
          ): AttributeModifications = {
            val existingAttrSize = existingAttrsToRecordIds.get(name).map(_.size).getOrElse(0)
            attribute match {
              case list: AttributeList[_] => checkAndUpdateRecSize(name, existingAttrSize, list.list.size, attrRecords)
              case _                      => checkAndUpdateRecSize(name, existingAttrSize, 1, attrRecords)
            }
          }

          traceDBIOWithParent("lookupNotYetLoadedReferences", span)(_ =>
            lookupNotYetLoadedReferences(workspaceContext, entityRefsToLookup, Seq(entityRecord.toReference))
          ) flatMap { entityRefRecs =>
            val allTheEntityRefs = entityRefRecs ++ Seq(entityRecord) // re-add the current entity
            val refsToIds = allTheEntityRefs.map(e => e.toReference -> e.id).toMap

            // get ids that need to be deleted from db
            val deleteIds = (for {
              attributeName <- deletes
            } yield existingAttrsToRecordIds.get(attributeName)).flatten.flatten

            val (insertRecs: Seq[EntityAttributeRecord],
                 updateRecs: Seq[EntityAttributeRecord],
                 extraDeleteIds: Seq[Long]
            ) = upserts
              .map { case (name, attribute) =>
                val attrRecords =
                  entityAttributeShardQuery(workspaceContext).marshalAttribute(refsToIds(entityRecord.toReference),
                                                                               name,
                                                                               attribute,
                                                                               refsToIds
                  )

                recordsForUpdateAttribute(name, attribute, attrRecords)
              }
              .foldLeft((Seq.empty[EntityAttributeRecord], Seq.empty[EntityAttributeRecord], Seq.empty[Long])) {
                case ((insert1, update1, delete1), (insert2, update2, delete2)) =>
                  (insert1 ++ insert2, update1 ++ update2, delete1 ++ delete2)
              }

            val totalDeleteIds = deleteIds ++ extraDeleteIds

            entityAttributeShardQuery(workspaceContext).patchAttributesAction(
              insertRecs,
              updateRecs,
              totalDeleteIds,
              entityAttributeTempQuery.insertScratchAttributes,
              span
            )
          }
        }
      }
    }

    // "patch" this entity by applying the upserts and the deletes to its attributes, then save. a little more database efficient than a "full" save, but requires the entity already exist.
    def saveEntityPatch(workspaceContext: Workspace,
                        entityRef: AttributeEntityReference,
                        upserts: AttributeMap,
                        deletes: Traversable[AttributeName],
                        tracingContext: RawlsTracingContext
    ) =
      traceDBIOWithParent("saveEntityPatch", tracingContext) { span =>
        setTraceSpanAttribute(span, AttributeKey.stringKey("workspaceId"), workspaceContext.workspaceId)
        setTraceSpanAttribute(span, AttributeKey.stringKey("workspace"), workspaceContext.toWorkspaceName.toString)
        setTraceSpanAttribute(span, AttributeKey.stringKey("entityType"), entityRef.entityType)
        setTraceSpanAttribute(span, AttributeKey.stringKey("entityName"), entityRef.entityName)
        setTraceSpanAttribute(span, AttributeKey.longKey("numUpserts"), java.lang.Long.valueOf(upserts.size))
        setTraceSpanAttribute(span, AttributeKey.longKey("numDeletes"), java.lang.Long.valueOf(deletes.size))
        val deleteIntersectUpsert = deletes.toSet intersect upserts.keySet
        if (upserts.isEmpty && deletes.isEmpty) {
          DBIO.successful(()) // no-op
        } else if (deleteIntersectUpsert.nonEmpty) {
          DBIO.failed(
            new RawlsException(
              s"Can't saveEntityPatch on $entityRef because upserts and deletes share attributes $deleteIntersectUpsert"
            )
          )
        } else {
          traceDBIOWithParent("getEntityRecords", span)(_ =>
            getEntityRecords(workspaceContext.workspaceIdAsUUID, Set(entityRef))
          ) flatMap { entityRecs =>
            if (entityRecs.length != 1) {
              throw new RawlsException(
                s"saveEntityPatch looked up $entityRef expecting 1 record, got ${entityRecs.length} instead"
              )
            }

            val entityRecord = entityRecs.head
            upserts.keys.foreach { attrName =>
              validateUserDefinedString(attrName.name)
              validateAttributeName(attrName, entityRecord.entityType)
            }

            for {
              _ <- applyEntityPatch(workspaceContext, entityRecord, upserts, deletes, span)
              updatedEntities <- traceDBIOWithParent("getEntities", span)(_ =>
                entityQuery.getEntities(workspaceContext.workspaceIdAsUUID, Seq(entityRecord.id))
              )
              _ <- traceDBIOWithParent("optimisticLockUpdate", span)(_ =>
                entityQueryWithInlineAttributes.optimisticLockUpdate(entityRecs, updatedEntities.map(elem => elem._2))
              )
            } yield {}
          }
        }
      }

    private def lookupNotYetLoadedReferences(workspaceContext: Workspace,
                                             entities: Traversable[Entity],
                                             alreadyLoadedEntityRefs: Seq[AttributeEntityReference]
    ): ReadAction[Seq[EntityRecord]] = {
      val allRefAttributes = (for {
        entity <- entities
        (_, attribute) <- entity.attributes
        ref <- attribute match {
          case AttributeEntityReferenceList(l) => l
          case r: AttributeEntityReference     => Seq(r)
          case _                               => Seq.empty
        }
      } yield ref).toSet

      lookupNotYetLoadedReferences(workspaceContext, allRefAttributes, alreadyLoadedEntityRefs)
    }

    private def lookupNotYetLoadedReferences(workspaceContext: Workspace,
                                             attrReferences: Set[AttributeEntityReference],
                                             alreadyLoadedEntityRefs: Seq[AttributeEntityReference]
    ): ReadAction[Seq[EntityRecord]] = {
      val notYetLoadedEntityRecs = attrReferences -- alreadyLoadedEntityRefs

      getEntityRecords(workspaceContext.workspaceIdAsUUID, notYetLoadedEntityRecs) map { foundEntities =>
        if (foundEntities.size != notYetLoadedEntityRecs.size) {
          val notFoundRefs = notYetLoadedEntityRecs -- foundEntities.map(_.toReference)
          throw new RawlsExceptionWithErrorReport(
            ErrorReport(
              StatusCodes.BadRequest,
              "Could not resolve some entity references",
              notFoundRefs.map { missingRef =>
                ErrorReport(s"${missingRef.entityType} ${missingRef.entityName} not found", Seq.empty)
              }.toSeq
            )
          )
        } else {
          foundEntities
        }
      }
    }

    private def rewriteAttributes(workspaceId: UUID,
                                  entitiesToSave: Traversable[Entity],
                                  entityIds: Seq[Long],
                                  entityIdsByRef: Map[AttributeEntityReference, Long]
    ) = {
      val attributesToSave = for {
        entity <- entitiesToSave
        (attributeName, attribute) <- entity.attributes
        attributeRec <- entityAttributeShardQuery(workspaceId).marshalAttribute(entityIdsByRef(entity.toReference),
                                                                                attributeName,
                                                                                attribute,
                                                                                entityIdsByRef
        )
      } yield attributeRec

      entityAttributeShardQuery(workspaceId).findByOwnerQuery(entityIds).result flatMap { existingAttributes =>
        entityAttributeShardQuery(workspaceId).rewriteAttrsAction(attributesToSave,
                                                                  existingAttributes,
                                                                  entityAttributeTempQuery.insertScratchAttributes
        )
      }
    }

    // "delete" entities by hiding and renaming. we must rename the entity to avoid future name collisions if the user
    // attempts to create a new entity of the same name.
    def hide(workspaceContext: Workspace, entRefs: Seq[AttributeEntityReference]): ReadWriteAction[Int] =
      // N.B. we must hide both the entity attributes and the entity itself. Other queries, such
      // as baseEntityAndAttributeSql, use "where ENTITY.deleted = ENTITY_ATTRIBUTE.deleted" during joins.
      // Thus, we need to keep the "deleted" value for attributes in sync with their parent entity,
      // when hiding that entity.
      workspaceQuery.updateLastModified(workspaceContext.workspaceIdAsUUID) andThen
        EntityAndAttributesRawSqlQuery.batchHide(workspaceContext, entRefs) andThen
        EntityRecordRawSqlQuery.batchHide(workspaceContext.workspaceIdAsUUID, entRefs).map(res => res.sum)

    // "deletes" entities of a certain type by hiding and renaming them
    def hideType(workspaceContext: Workspace, entityType: String): ReadWriteAction[Int] =
      for {
        _ <- EntityAndAttributesRawSqlQuery.batchHideAttributesOfType(workspaceContext, entityType)
        numEntitiesHidden <- EntityRecordRawSqlQuery.batchHideType(workspaceContext.workspaceIdAsUUID, entityType)
        _ <- workspaceQuery.updateLastModified(workspaceContext.workspaceIdAsUUID)
      } yield numEntitiesHidden.sum

    // perform actual deletion (not hiding) of all entities in a workspace

    def deleteFromDb(workspaceContext: Workspace): WriteAction[Int] =
      EntityDependenciesDeletionQuery.deleteAction(workspaceContext) andThen {
        filter(_.workspaceId === workspaceContext.workspaceIdAsUUID).delete
      }

    def countReferringEntitiesForType(workspaceContext: Workspace, entityType: String): ReadAction[Int] =
      EntityAndAttributesRawSqlQuery.countReferencesToType(workspaceContext, entityType).map(_.sum)

    def copyEntitiesToNewWorkspace(sourceWs: UUID,
                                   destWs: UUID,
                                   entityRefs: Set[AttributeEntityReference] = Set()
    ): WriteAction[(Int, Int)] = {

      def copyChunkOfEntitiesOrAllEntities(chunk: Set[AttributeEntityReference] = Set()) =
        for {
          entitiesCopiedCount <- CopyEntitiesQuery.copyEntities(sourceWs, destWs, chunk)
          attributesCopiedCount <- CopyEntitiesQuery.copyAttributes(sourceWs, destWs, chunk)
        } yield (entitiesCopiedCount, attributesCopiedCount)

      val chunks: Iterator[Set[AttributeEntityReference]] = if (entityRefs.size > batchSize) {
        entityRefs.grouped(batchSize)
      } else {
        Iterator(entityRefs)
      }

      val allCopies = DBIO.sequence(chunks map copyChunkOfEntitiesOrAllEntities)

      allCopies.map { copyActionResults: Iterator[(Int, Int)] =>
        (copyActionResults.map(_._1).sum, copyActionResults.map(_._2).sum)
      }
    }

    def doesEntityTypeAlreadyExist(workspaceContext: Workspace, entityType: String): ReadAction[Option[Boolean]] =
      uniqueResult(CheckForExistingEntityTypeQuery.doesEntityTypeAlreadyExist(workspaceContext, entityType))

    def changeEntityTypeName(workspaceContext: Workspace, oldName: String, newName: String): ReadWriteAction[Int] =
      for {
        numRowsRenamed <- ChangeEntityTypeNameQuery.changeEntityTypeName(workspaceContext, oldName, newName)
        _ <- workspaceQuery.updateLastModified(workspaceContext.workspaceIdAsUUID)
      } yield numRowsRenamed

    def rename(workspaceContext: Workspace,
               entityType: String,
               oldName: String,
               newName: String
    ): ReadWriteAction[Int] = {
      validateEntityName(newName)
      workspaceQuery.updateLastModified(workspaceContext.workspaceIdAsUUID) andThen
        findEntityByName(workspaceContext.workspaceIdAsUUID, entityType, oldName).map(_.name).update(newName)
    }

    // copy entities from one workspace to another, checking for conflicts first

    def checkAndCopyEntities(sourceWorkspaceContext: Workspace,
                             destWorkspaceContext: Workspace,
                             entityType: String,
                             entityNames: Seq[String],
                             linkExistingEntities: Boolean,
                             parentContext: RawlsRequestContext
    ): ReadWriteAction[EntityCopyResponse] = {

      def getSoftConflicts(paths: Seq[EntityPath]) =
        getCopyConflicts(destWorkspaceContext, paths.map(_.path.last)).map { conflicts =>
          val conflictsAsRefs = conflicts.toSeq.map(_.toReference)
          paths.filter(p => conflictsAsRefs.contains(p.path.last))
        }

      def buildSoftConflictTree(pathsRemaining: EntityPath): Seq[EntitySoftConflict] =
        if (pathsRemaining.path.isEmpty) Seq.empty
        else
          Seq(
            EntitySoftConflict(pathsRemaining.path.head.entityType,
                               pathsRemaining.path.head.entityName,
                               buildSoftConflictTree(EntityPath(pathsRemaining.path.tail))
            )
          )

      val entitiesToCopyRefs = entityNames.map(name => AttributeEntityReference(entityType, name))

      traceDBIOWithParent("EntityComponent.checkAndCopyEntities", parentContext) { childContext =>
        setTraceSpanAttribute(childContext, AttributeKey.stringKey("destWorkspaceId"), destWorkspaceContext.workspaceId)
        setTraceSpanAttribute(childContext,
                              AttributeKey.stringKey("sourceWorkspaceId"),
                              destWorkspaceContext.toWorkspaceName.toString
        )
        setTraceSpanAttribute(childContext,
                              AttributeKey.longKey("numEntities"),
                              java.lang.Long.valueOf(entityNames.length)
        )
        traceDBIOWithParent("getHardConflicts", childContext)(s2 =>
          getActiveRefs(destWorkspaceContext.workspaceIdAsUUID, entitiesToCopyRefs.toSet).flatMap {
            case Seq() =>
              val pathsAndConflicts = for {
                entityPaths <- traceDBIOWithParent("getEntitySubtrees", s2)(_ =>
                  getEntitySubtrees(sourceWorkspaceContext, entityType, entityNames.toSet)
                )
                softConflicts <- traceDBIOWithParent("getSoftConflicts", s2)(_ => getSoftConflicts(entityPaths))
              } yield (entityPaths, softConflicts)

              pathsAndConflicts.flatMap { case (entityPaths, softConflicts) =>
                if (softConflicts.isEmpty || linkExistingEntities) {
                  val allEntityRefs = entityPaths.flatMap(_.path)
                  val allConflictRefs = softConflicts.flatMap(_.path)
                  val entitiesToCopy = allEntityRefs diff allConflictRefs toSet

                  for {
                    _ <- traceDBIOWithParent("copyEntities", s2)(_ =>
                      copyEntitiesToNewWorkspace(sourceWorkspaceContext.workspaceIdAsUUID,
                                                 destWorkspaceContext.workspaceIdAsUUID,
                                                 entitiesToCopy
                      )
                    )
                    _ <- workspaceQuery.updateLastModified(destWorkspaceContext.workspaceIdAsUUID)
                  } yield EntityCopyResponse(entitiesToCopy.toSeq, Seq.empty, Seq.empty)
                } else {
                  val unmergedSoftConflicts = softConflicts
                    .flatMap(buildSoftConflictTree)
                    .groupBy(c => (c.entityType, c.entityName))
                    .map { case ((conflictType, conflictName), conflicts) =>
                      EntitySoftConflict(conflictType, conflictName, conflicts.flatMap(_.conflicts))
                    }
                    .toSeq
                  DBIO.successful(EntityCopyResponse(Seq.empty, Seq.empty, unmergedSoftConflicts))
                }
              }
            case hardConflicts =>
              DBIO.successful(
                EntityCopyResponse(Seq.empty,
                                   hardConflicts.map(c => EntityHardConflict(c.entityType, c.entityName)),
                                   Seq.empty
                )
              )
          }
        )
      }
    }

    // retrieve all paths from these entities, for checkAndCopyEntities

    private[slick] def getEntitySubtrees(workspaceContext: Workspace,
                                         entityType: String,
                                         entityNames: Set[String]
    ): ReadAction[Seq[EntityPath]] = {
      val startingEntityRecsAction = filter(rec =>
        rec.workspaceId === workspaceContext.workspaceIdAsUUID && rec.entityType === entityType && rec.name.inSetBind(
          entityNames
        )
      )

      startingEntityRecsAction.result.flatMap { startingEntityRecs =>
        val refsToId = startingEntityRecs.map(rec => EntityPath(Seq(rec.toReference)) -> rec.id).toMap
        recursiveGetEntityReferences(workspaceContext, Down, startingEntityRecs.map(_.id).toSet, refsToId)
      }
    }

    // return the entities already present in the destination workspace

    private[slick] def getCopyConflicts(destWorkspaceContext: Workspace,
                                        entitiesToCopy: TraversableOnce[AttributeEntityReference]
    ): ReadAction[TraversableOnce[EntityRecord]] =
      getEntityRecords(destWorkspaceContext.workspaceIdAsUUID, entitiesToCopy.toSet)

    // the opposite of getEntitySubtrees: traverse the graph to retrieve all entities which ultimately refer to these

    def getAllReferringEntities(context: Workspace,
                                entities: Set[AttributeEntityReference]
    ): ReadAction[Set[AttributeEntityReference]] =
      getEntityRecords(context.workspaceIdAsUUID, entities) flatMap { entityRecs =>
        val refsToId = entityRecs.map(rec => EntityPath(Seq(rec.toReference)) -> rec.id).toMap
        recursiveGetEntityReferences(context, Up, entityRecs.map(_.id).toSet, refsToId)
      } flatMap { refs =>
        val entityAction =
          EntityAndAttributesRawSqlQuery.activeActionForRefs(context, refs.flatMap(_.path).toSet) map (query =>
            unmarshalEntities(query)
          )
        entityAction map { _.toSet map { e: Entity => e.toReference } }
      }

    sealed trait RecursionDirection
    case object Up extends RecursionDirection
    case object Down extends RecursionDirection

    /**
      * Starting with entities specified by entityIds, recursively walk references accumulating all the ids
      * @param direction whether to walk Down (my object references others) or Up (my object is referenced by others) the graph
      * @param entityIds the ids to start with
      * @param accumulatedPathsWithLastId the ids accumulated from the prior call. If you wish entityIds to be in the overall
      *                       results, start with entityIds == accumulatedIds, otherwise start with Seq.empty but note
      *                       that if there is a cycle some of entityIds may be in the result anyway
      * @return the ids of all the entities referred to by entityIds
      */
    private def recursiveGetEntityReferences(workspace: Workspace,
                                             direction: RecursionDirection,
                                             entityIds: Set[Long],
                                             accumulatedPathsWithLastId: Map[EntityPath, Long]
    ): ReadAction[Seq[EntityPath]] = {
      def oneLevelDown(idBatch: Set[Long]): ReadAction[Set[(Long, EntityRecord)]] = {
        val query = entityAttributeShardQuery(workspace) filter (_.ownerId inSetBind idBatch) join
          this on { (attr, ent) => attr.valueEntityRef === ent.id && !attr.deleted } map { case (attr, entity) =>
            (attr.ownerId, entity)
          }
        query.result.map(_.toSet)
      }

      def oneLevelUp(idBatch: Set[Long]): ReadAction[Set[(Long, EntityRecord)]] = {
        val query = entityAttributeShardQuery(workspace) filter (_.valueEntityRef inSetBind idBatch) join
          this on { (attr, ent) => attr.ownerId === ent.id && !ent.deleted } map { case (attr, entity) =>
            (attr.valueEntityRef.get, entity)
          }
        query.result.map(_.toSet)
      }

      // need to batch because some RDBMSes have a limit on the length of an in clause
      val batchedEntityIds: Iterator[Set[Long]] = entityIds.grouped(batchSize)

      val batchActions: Iterator[ReadAction[Set[(Long, EntityRecord)]]] = direction match {
        case Down => batchedEntityIds map oneLevelDown
        case Up   => batchedEntityIds map oneLevelUp
      }

      DBIO.sequence(batchActions).map(_.flatten.toSet).flatMap { priorIdWithCurrentRec =>
        val currentPaths = priorIdWithCurrentRec.flatMap { case (priorId, currentRec) =>
          val pathsThatEndWithPrior = accumulatedPathsWithLastId.filter { case (_, id) => id == priorId }
          pathsThatEndWithPrior.keys.map(_.path).map { path =>
            (EntityPath(path :+ currentRec.toReference), currentRec.id)
          }
        }.toMap

        val untraversedIds = priorIdWithCurrentRec.map(_._2.id) -- accumulatedPathsWithLastId.values
        if (untraversedIds.isEmpty) {
          DBIO.successful(accumulatedPathsWithLastId.keys.toSeq)
        } else {
          recursiveGetEntityReferences(workspace, direction, untraversedIds, accumulatedPathsWithLastId ++ currentPaths)
        }
      }
    }

    // Utility methods

    private def validateEntity(entity: Entity): Unit = {
      if (entity.entityType.equalsIgnoreCase(Attributable.workspaceEntityType)) {
        throw new RawlsFatalExceptionWithErrorReport(
          errorReport = ErrorReport(
            message = s"Entity type ${Attributable.workspaceEntityType} is reserved and cannot be overwritten",
            statusCode = StatusCodes.BadRequest
          )
        )
      }
      validateUserDefinedString(entity.entityType)
      validateEntityName(entity.name)
      entity.attributes.keys.foreach { attrName =>
        validateUserDefinedString(attrName.name)
        validateAttributeName(attrName, entity.entityType)
      }
    }

    def generateEntityMetadataMap(typesAndCountsQ: ReadAction[Map[String, Int]],
                                  typesAndAttrsQ: ReadAction[Map[String, Seq[AttributeName]]]
    ) =
      typesAndCountsQ flatMap { typesAndCounts =>
        typesAndAttrsQ map { typesAndAttrs =>
          (typesAndCounts.keySet ++ typesAndAttrs.keySet) map { entityType =>
            (entityType,
             EntityTypeMetadata(
               typesAndCounts.getOrElse(entityType, 0),
               entityType + Attributable.entityIdAttributeSuffix,
               typesAndAttrs.getOrElse(entityType, Seq()).map(AttributeName.toDelimitedName).sortBy(_.toLowerCase)
             )
            )
          } toMap
        }
      }

    // Unmarshal methods

    // NOTE: marshalNewEntity is in entityQueryWithInlineAttributes because it helps save the inline attributes to DB

    private def unmarshalEntity(entityRecord: EntityRecord, attributes: AttributeMap): Entity =
      Entity(entityRecord.name, entityRecord.entityType, attributes)

    def unmarshalEntities(
      entityAttributeRecords: Seq[EntityAndAttributesResult]
    ): Seq[Entity] =
      unmarshalEntitiesWithIds(entityAttributeRecords).map { case (_, entity) => entity }

    def unmarshalEntitiesWithIds(
      entityAttributeRecords: Seq[EntityAndAttributesResult]
    ): Seq[(Long, Entity)] = {
      val allEntityRecords = entityAttributeRecords.map(_.entityRecord).distinct

      // note that not all entities have attributes, thus the collect below
      val entitiesWithAttributes = entityAttributeRecords.collect {
        case EntityAndAttributesResult(entityRec, Some(attributeRec), refEntityRecOption) =>
          ((entityRec.id, attributeRec), refEntityRecOption)
      }

      val attributesByEntityId =
        entityAttributeShardQuery(UUID.randomUUID()).unmarshalAttributes(entitiesWithAttributes)

      allEntityRecords.map { entityRec =>
        entityRec.id -> unmarshalEntity(entityRec, attributesByEntityId.getOrElse(entityRec.id, Map.empty))
      }
    }
  }

  object entityCacheManagementQuery {

    // given a workspace and entity metadata, persist that metadata to the cache tables
    def saveEntityCache(workspaceId: UUID,
                        entityTypesWithCounts: Map[String, Int],
                        entityTypesWithAttrNames: Map[String, Seq[AttributeName]],
                        timestamp: Timestamp
    ) =
      // TODO: beware contention on the approach of delete-all and batch-insert all below
      // if we see contention we could move to encoding the entire metadata object as json
      // and storing in a single column on WORKSPACE_ENTITY_CACHE
      for {
        // update entity statistics
        _ <- entityTypeStatisticsQuery.deleteAllForWorkspace(workspaceId)
        _ <- entityTypeStatisticsQuery.batchInsert(workspaceId, entityTypesWithCounts)
        // update entity attribute statistics
        _ <- entityAttributeStatisticsQuery.deleteAllForWorkspace(workspaceId)
        _ <- entityAttributeStatisticsQuery.batchInsert(workspaceId, entityTypesWithAttrNames)
        // update cache update date
        numCachesUpdated <- entityCacheQuery.updateCacheLastUpdated(workspaceId, timestamp)
      } yield numCachesUpdated

  }
}
