package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.sql.Timestamp
import java.util.{Date, UUID}

import org.broadinstitute.dsde.rawls.util.CollectionUtils
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport, model}
import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model._
import slick.dbio.DBIOAction
import slick.dbio.Effect.{Read, Write}
import slick.driver.JdbcDriver
import slick.jdbc.GetResult
import spray.http.StatusCodes

/**
 * Created by dvoet on 2/4/16.
 */
case class EntityRecord(id: Long,
                        name: String,
                        entityType: String,
                        workspaceId: UUID,
                        recordVersion: Long,
                        allAttributeValues: Option[String],
                        deleted: Boolean,
                        deletedDate: Option[Timestamp]) {
  def toReference = AttributeEntityReference(entityType, name)
}

trait EntityComponent {
  this: DriverComponent
    with WorkspaceComponent
    with AttributeComponent =>

  import driver.api._

  class EntityTable(tag: Tag) extends Table[EntityRecord](tag, "ENTITY") {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def name = column[String]("name", O.Length(254))
    def entityType = column[String]("entity_type", O.Length(254))
    def workspaceId = column[UUID]("workspace_id")
    def version = column[Long]("record_version")
    def allAttributeValues = column[Option[String]]("all_attribute_values")
    def deleted = column[Boolean]("deleted")
    def deletedDate = column[Option[Timestamp]]("deleted_date")

    def workspace = foreignKey("FK_ENTITY_WORKSPACE", workspaceId, workspaceQuery)(_.id)
    def uniqueTypeName = index("idx_entity_type_name", (workspaceId, entityType, name), unique = true)

    def * = (id, name, entityType, workspaceId, version, allAttributeValues, deleted, deletedDate) <> (EntityRecord.tupled, EntityRecord.unapply)
  }

  object entityQuery extends TableQuery(new EntityTable(_)) {

    type EntityQuery = Query[EntityTable, EntityRecord, Seq]
    type EntityAttributeQuery = Query[EntityAttributeTable, EntityAttributeRecord, Seq]

    // Raw queries - used when querying for multiple AttributeEntityReferences

    private object EntityRecordRawSqlQuery extends RawSqlQuery {
      val driver: JdbcDriver = EntityComponent.this.driver
      implicit val getEntityRecord = GetResult { r => EntityRecord(r.<<, r.<<, r.<<, r.<<, r.<<, None, r.<<, r.<<) }

      def action(workspaceId: UUID, entities: Set[AttributeEntityReference]): ReadAction[Seq[EntityRecord]] = {
        if( entities.isEmpty ) {
          DBIO.successful(Seq.empty[EntityRecord])
        } else {
          val baseSelect = sql"select id, name, entity_type, workspace_id, record_version, deleted, deleted_date from ENTITY where workspace_id = $workspaceId and (entity_type, name) in ("
          val entityTypeNameTuples = reduceSqlActionsWithDelim(entities.map { entity => sql"(${entity.entityType}, ${entity.entityName})" }.toSeq)
          concatSqlActions(baseSelect, entityTypeNameTuples, sql")").as[EntityRecord]
        }
      }
    }

    private object EntityAndAttributesRawSqlQuery extends RawSqlQuery {
      val driver: JdbcDriver = EntityComponent.this.driver

      // result structure from entity and attribute list raw sql
      case class EntityAndAttributesResult(entityRecord: EntityRecord, attributeRecord: Option[EntityAttributeRecord], refEntityRecord: Option[EntityRecord])

      // tells slick how to convert a result row from a raw sql query to an instance of EntityAndAttributesResult
      implicit val getEntityAndAttributesResult = GetResult { r =>
        // note that the number and order of all the r.<< match precisely with the select clause of baseEntityAndAttributeSql
        val entityRec = EntityRecord(r.<<, r.<<, r.<<, r.<<, r.<<, None, r.<<, r.<<)

        val attributeIdOption: Option[Long] = r.<<
        val attributeRecOption = attributeIdOption.map(id => EntityAttributeRecord(id, entityRec.id, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

        val refEntityRecOption = for {
          attributeRec <- attributeRecOption
          refId <- attributeRec.valueEntityRef
        } yield {
          EntityRecord(r.<<, r.<<, r.<<, r.<<, r.<<, None, r.<<, r.<<)
        }

        EntityAndAttributesResult(entityRec, attributeRecOption, refEntityRecOption)
      }

      // the where clause for this query is filled in specific to the use case
      val baseEntityAndAttributeSql =
        s"""select e.id, e.name, e.entity_type, e.workspace_id, e.record_version, e.deleted, e.deleted_date,
          a.id, a.namespace, a.name, a.value_string, a.value_number, a.value_boolean, a.value_json, a.value_entity_ref, a.list_index, a.list_length, a.deleted, a.deleted_date,
          e_ref.id, e_ref.name, e_ref.entity_type, e_ref.workspace_id, e_ref.record_version, e_ref.deleted, e_ref.deleted_date
          from ENTITY e
          left outer join ENTITY_ATTRIBUTE a on a.owner_id = e.id and a.deleted = e.deleted
          left outer join ENTITY e_ref on a.value_entity_ref = e_ref.id"""

      // Active actions: only return entities and attributes with their deleted flag set to false

      def activeActionForType(workspaceContext: SlickWorkspaceContext, entityType: String): ReadAction[Seq[EntityAndAttributesResult]] = {
        sql"""#$baseEntityAndAttributeSql where e.deleted = false and e.entity_type = ${entityType} and e.workspace_id = ${workspaceContext.workspaceId}""".as[EntityAndAttributesResult]
      }

      def activeActionForRefs(workspaceContext: SlickWorkspaceContext, entityRefs: Set[AttributeEntityReference]): ReadAction[Seq[EntityAndAttributesResult]] = {
        if( entityRefs.isEmpty ) {
          DBIO.successful(Seq.empty[EntityAndAttributesResult])
        } else {
          val baseSelect = sql"""#$baseEntityAndAttributeSql where e.deleted = false and e.workspace_id = ${workspaceContext.workspaceId} and (e.entity_type, e.name) in ("""
          val entityTypeNameTuples = reduceSqlActionsWithDelim(entityRefs.map { ref => sql"(${ref.entityType}, ${ref.entityName})" }.toSeq)
          concatSqlActions(baseSelect, entityTypeNameTuples, sql")").as[EntityAndAttributesResult]
        }

      }

      def activeActionForWorkspace(workspaceContext: SlickWorkspaceContext): ReadAction[Seq[EntityAndAttributesResult]] = {
        sql"""#$baseEntityAndAttributeSql where e.deleted = false and e.workspace_id = ${workspaceContext.workspaceId}""".as[EntityAndAttributesResult]
      }

      /**
        * Generates a sub query that can be filtered, sorted, sliced
        * @param workspaceId
        * @param entityType
        * @param sortFieldName
        * @return
        */
      private def paginationSubquery(workspaceId: UUID, entityType: String, sortFieldName: String) = {

        val (sortColumns, sortJoin) = sortFieldName match {
          case "name" => (
            // no additional sort columns
            "",
            // no additional join required
            sql"")
          case _ => (
            // select each attribute column and the referenced entity name
            """, sort_a.list_length as sort_list_length, sort_a.value_string as sort_field_string, sort_a.value_number as sort_field_number, sort_a.value_boolean as sort_field_boolean, sort_a.value_json as sort_field_json, sort_e_ref.name as sort_field_ref""",
            // join to attribute and entity (for references) table, grab only the named sort attribute and only the first element of a list
            sql"""left outer join ENTITY_ATTRIBUTE sort_a on sort_a.owner_id = e.id and sort_a.name = $sortFieldName and ifnull(sort_a.list_index, 0) = 0 left outer join ENTITY sort_e_ref on sort_a.value_entity_ref = sort_e_ref.id """)
        }

        concatSqlActions(sql"""select e.id, e.name, e.all_attribute_values #$sortColumns from ENTITY e """, sortJoin, sql""" where e.deleted = 'false' and e.entity_type = $entityType and e.workspace_id = $workspaceId """)
      }

      def activeActionForPagination(workspaceContext: SlickWorkspaceContext, entityType: String, entityQuery: model.EntityQuery): ReadAction[(Int, Int, Seq[EntityAndAttributesResult])] = {
        /*
        The query here starts with baseEntityAndAttributeSql which is the typical select
        to pull entities will all attributes and references. A join is added on a sub select from ENTITY
        (and ENTITY_ATTRIBUTE if there is a sort field other than name) which constrains to only entities of the
        right type, workspace, and matches the filters then sorts and slices out the appropriate page of entities.
         */

        def filterSql(prefix: String, alias: String) = {
          val filtersOption = entityQuery.filterTerms.map { _.split(" ").toSeq.map { term =>
            sql"concat(#$alias.name, ' ', #$alias.all_attribute_values) like ${'%' + term.toLowerCase + '%'}"
          }}

          filtersOption match {
            case None => sql""
            case Some(filters) =>
              concatSqlActions(sql"#$prefix ", reduceSqlActionsWithDelim(filters, sql" and "))
          }
        }

        def order(alias: String) = entityQuery.sortField match {
          case "name" => sql" order by #$alias.name #${SortDirections.toSql(entityQuery.sortDirection)} "
          case _ => sql" order by #$alias.sort_list_length #${SortDirections.toSql(entityQuery.sortDirection)}, #$alias.sort_field_string #${SortDirections.toSql(entityQuery.sortDirection)}, #$alias.sort_field_number #${SortDirections.toSql(entityQuery.sortDirection)}, #$alias.sort_field_boolean #${SortDirections.toSql(entityQuery.sortDirection)}, #$alias.sort_field_ref #${SortDirections.toSql(entityQuery.sortDirection)}, #$alias.name #${SortDirections.toSql(entityQuery.sortDirection)} "
        }

        val paginationJoin = concatSqlActions(
          sql""" join (select * from (""",
          paginationSubquery(workspaceContext.workspaceId, entityType, entityQuery.sortField),
          sql") pagination ",
          filterSql("where", "pagination"),
          order("pagination"),
          sql" limit #${entityQuery.pageSize} offset #${(entityQuery.page-1) * entityQuery.pageSize} ) p on p.id = e.id "
        )

        for {
          filteredCount <- concatSqlActions(sql"select count(1) from (", paginationSubquery(workspaceContext.workspaceId, entityType, entityQuery.sortField), sql") pagination ", filterSql("where", "pagination")).as[Int]
          unfilteredCount <- findActiveEntityByType(workspaceContext.workspaceId, entityType).length.result
          page <- concatSqlActions(sql"#$baseEntityAndAttributeSql", paginationJoin, order("p")).as[EntityAndAttributesResult]
        } yield (unfilteredCount, filteredCount.head, page)
      }

      // actions which may include "deleted" hidden entities

      def actionForTypeName(workspaceContext: SlickWorkspaceContext, entityType: String, entityName: String): ReadAction[Seq[EntityAndAttributesResult]] = {
        sql"""#$baseEntityAndAttributeSql where e.name = ${entityName} and e.entity_type = ${entityType} and e.workspace_id = ${workspaceContext.workspaceId}""".as[EntityAndAttributesResult]
      }

      def actionForIds(entityIds: Set[Long]): ReadAction[Seq[EntityAndAttributesResult]] = {
        if( entityIds.isEmpty ) {
          DBIO.successful(Seq.empty[EntityAndAttributesResult])
        } else {
          val baseSelect = sql"""#$baseEntityAndAttributeSql where e.id in ("""
          val entityIdSql = reduceSqlActionsWithDelim(entityIds.map { id => sql"$id" }.toSeq)
          concatSqlActions(baseSelect, entityIdSql, sql")").as[EntityAndAttributesResult]
        }
      }

      def actionForWorkspace(workspaceContext: SlickWorkspaceContext): ReadAction[Seq[EntityAndAttributesResult]] = {
        sql"""#$baseEntityAndAttributeSql where e.workspace_id = ${workspaceContext.workspaceId}""".as[EntityAndAttributesResult]
      }
    }

    // Raw query for performing actual deletion (not hiding) of everything that depends on an entity

    private object EntityDependenciesDeletionQuery extends RawSqlQuery {
      val driver: JdbcDriver = EntityComponent.this.driver

      def deleteAction(workspaceId: UUID): WriteAction[Int] =
        sqlu"""delete ea from ENTITY_ATTRIBUTE ea
               inner join ENTITY e
               on ea.owner_id = e.id
               where e.workspace_id=${workspaceId}
          """
    }

    // Slick queries

    // Active queries: only return entities and attributes with their deleted flag set to false

    def findActiveEntityByType(workspaceId: UUID, entityType: String): EntityQuery = {
      filter(entRec => entRec.entityType === entityType && entRec.workspaceId === workspaceId && ! entRec.deleted)
    }

    def findActiveEntityByWorkspace(workspaceId: UUID): EntityQuery = {
      filter(entRec => entRec.workspaceId === workspaceId && ! entRec.deleted)
    }

    private def findActiveAttributesByEntityId(entityId: Rep[Long]): EntityAttributeQuery = for {
      entityAttrRec <- entityAttributeQuery if entityAttrRec.ownerId === entityId && ! entityAttrRec.deleted
    } yield entityAttrRec

    // queries which may include "deleted" hidden entities

    def findEntityByName(workspaceId: UUID, entityType: String, entityName: String): EntityQuery = {
      filter(entRec => entRec.name === entityName && entRec.entityType === entityType && entRec.workspaceId === workspaceId)
    }

    def findEntityById(id: Long): EntityQuery = {
      filter(_.id === id)
    }

    private def findEntityByIdAndVersion(id: Long, version: Long): EntityQuery = {
      filter(rec => rec.id === id && rec.version === version)
    }

    // Actions

    // get a specific entity or set of entities: may include "hidden" deleted entities if not named "active"

    def get(workspaceContext: SlickWorkspaceContext, entityType: String, entityName: String): ReadAction[Option[Entity]] = {
      EntityAndAttributesRawSqlQuery.actionForTypeName(workspaceContext, entityType, entityName) map unmarshalEntities map(_.headOption)
    }

    def getEntities(entityIds: Traversable[Long]): ReadAction[Seq[(Long, Entity)]] = {
      EntityAndAttributesRawSqlQuery.actionForIds(entityIds.toSet) map unmarshalEntitiesWithIds
    }

    def getEntityRecords(workspaceId: UUID, entities: Set[AttributeEntityReference]): ReadAction[Seq[EntityRecord]] = {
      val entitiesGrouped = entities.grouped(batchSize).toSeq

      DBIO.sequence(entitiesGrouped map { batch =>
        EntityRecordRawSqlQuery.action(workspaceId, batch)
      }).map(_.flatten)
    }

    def getActiveEntities(workspaceContext: SlickWorkspaceContext, entityRefs: Traversable[AttributeEntityReference]): ReadAction[TraversableOnce[Entity]] = {
      EntityAndAttributesRawSqlQuery.activeActionForRefs(workspaceContext, entityRefs.toSet) map unmarshalEntities
    }

    // list all entities or those in a category

    def listActiveEntities(workspaceContext: SlickWorkspaceContext): ReadAction[TraversableOnce[Entity]] = {
      EntityAndAttributesRawSqlQuery.activeActionForWorkspace(workspaceContext) map unmarshalEntities
    }

    // includes "deleted" hidden entities
    def listEntities(workspaceContext: SlickWorkspaceContext): ReadAction[TraversableOnce[Entity]] = {
      EntityAndAttributesRawSqlQuery.actionForWorkspace(workspaceContext) map unmarshalEntities
    }

    def listActiveEntitiesOfType(workspaceContext: SlickWorkspaceContext, entityType: String): ReadAction[TraversableOnce[Entity]] = {
      EntityAndAttributesRawSqlQuery.activeActionForType(workspaceContext, entityType) map unmarshalEntities
    }

    // get entity types, counts, and attribute names to populate UI tables.  Active entities and attributes only.

    def getEntityTypeMetadata(workspaceContext: SlickWorkspaceContext): ReadAction[Map[String, EntityTypeMetadata]] = {
      val typesAndCountsQ = getEntityTypesWithCounts(workspaceContext)
      val typesAndAttrsQ = getAttrNamesAndEntityTypes(workspaceContext)

      typesAndCountsQ flatMap { typesAndCounts =>
        typesAndAttrsQ map { typesAndAttrs =>
          (typesAndCounts.keySet ++ typesAndAttrs.keySet) map { entityType =>
            (entityType, EntityTypeMetadata( typesAndCounts.getOrElse(entityType, 0), entityType + Attributable.entityIdAttributeSuffix, typesAndAttrs.getOrElse(entityType, Seq()) ))
          } toMap
        }
      }
    }

    private[slick] def getEntityTypesWithCounts(workspaceContext: SlickWorkspaceContext): ReadAction[Map[String, Int]] = {
      findActiveEntityByWorkspace(workspaceContext.workspaceId).groupBy(e => e.entityType).map { case (entityType, entities) =>
        (entityType, entities.length)
      }.result map { result =>
        result.toMap
      }
    }

    private[slick] def getAttrNamesAndEntityTypes(workspaceContext: SlickWorkspaceContext): ReadAction[Map[String, Seq[String]]] = {
      val typesAndAttrNames = for {
        entityRec <- findActiveEntityByWorkspace(workspaceContext.workspaceId)
        attrib <- findActiveAttributesByEntityId(entityRec.id)
      } yield {
        (entityRec.entityType, attrib.name)
      }

      typesAndAttrNames.distinct.result map { result =>
        CollectionUtils.groupByTuples(result)
      }
    }

    // get paginated entities for UI display, as a result of executing a query

    def loadEntityPage(workspaceContext: SlickWorkspaceContext, entityType: String, entityQuery: model.EntityQuery): ReadAction[(Int, Int, Iterable[Entity])] = {
      EntityAndAttributesRawSqlQuery.activeActionForPagination(workspaceContext, entityType, entityQuery) map { case (unfilteredCount, filteredCount, pagination) =>
        (unfilteredCount, filteredCount, unmarshalEntities(pagination))
      }
    }

    // create or replace entities

    def save(workspaceContext: SlickWorkspaceContext, entity: Entity): ReadWriteAction[Entity] = {
      save(workspaceContext, Seq(entity)).map(_.head)
    }

    def save(workspaceContext: SlickWorkspaceContext, entities: Traversable[Entity]): ReadWriteAction[Traversable[Entity]] = {
      workspaceQuery.updateLastModified(workspaceContext.workspaceId)
      entities.foreach(validateEntity)

      for {
        preExistingEntityRecs <- getEntityRecords(workspaceContext.workspaceId, entities.map(_.toReference).toSet).map(populateAllAttributeValues(_, entities))
        savingEntityRecs <- insertNewEntities(workspaceContext, entities, preExistingEntityRecs).map(_ ++ preExistingEntityRecs)
        referencedAndSavingEntityRecs <- lookupNotYetLoadedReferences(workspaceContext, entities, savingEntityRecs).map(_ ++ savingEntityRecs)
        _ <- rewriteAttributes(entities, savingEntityRecs.map(_.id), referencedAndSavingEntityRecs.map(e => e.toReference -> e.id).toMap)
        _ <- DBIO.seq(preExistingEntityRecs map optimisticLockUpdate: _ *)
      } yield entities
    }

    private def applyEntityPatch(workspaceContext: SlickWorkspaceContext, entityRecord: EntityRecord, upserts: AttributeMap, deletes: Traversable[AttributeName]) = {
      //yank the attribute list for this entity to determine what to do with upserts
      entityAttributeQuery.findByOwnerQuery(Seq(entityRecord.id)).map(attr => (attr.namespace, attr.name, attr.id)).result flatMap { attrCols =>

        val existingAttrsToRecordIds: Map[AttributeName, Long] = attrCols.map(a => AttributeName(a._1, a._2) -> a._3).toMap
        val entityRefsToLookup = upserts.valuesIterator.collect { case e: AttributeEntityReference => e }.toSet

        lookupNotYetLoadedReferences(workspaceContext, entityRefsToLookup, Seq(entityRecord)) flatMap { entityRefRecs =>
          val allTheEntityRefs = entityRefRecs ++ Seq(entityRecord) //re-add the current entity
          val refsToIds = allTheEntityRefs.map(e => e.toReference -> e.id).toMap
          val existingAttributes = existingAttrsToRecordIds.keys.toSeq

          val (updateAttrs, insertAttrs) = upserts.partition( attr => existingAttributes.contains(attr._1) )

          //function that marshals attribute maps to list of attribute records for saving
          def attributeMapToRecs(attrMap: AttributeMap): Iterable[EntityAttributeRecord] = {
            for {
              (attributeName, attribute) <- attrMap
              attributeRec <- entityAttributeQuery.marshalAttribute(refsToIds(entityRecord.toReference), attributeName, attribute, refsToIds)
            } yield attributeRec
          }

          //actually build the records we're going to send to the db
          val insertRecs = attributeMapToRecs(insertAttrs)
          val updateRecs = attributeMapToRecs(updateAttrs)
          val deleteIds = (for {
            attributeName <- deletes
          } yield existingAttrsToRecordIds.get(attributeName)).flatten

          entityAttributeQuery.patchAttributesAction(insertRecs, updateRecs, deleteIds, entityAttributeScratchQuery.insertScratchAttributes)
        }
      }
    }

    //"patch" this entity by applying the upserts and the deletes to its attributes, then save. a little more database efficient than a "full" save, but requires the entity already exist.
    def saveEntityPatch(workspaceContext: SlickWorkspaceContext, entityRef: AttributeEntityReference, upserts: AttributeMap, deletes: Traversable[AttributeName]) = {
      val deleteIntersectUpsert = deletes.toSet intersect upserts.keySet
      if (upserts.isEmpty && deletes.isEmpty) {
        DBIO.successful(()) //no-op
      } else if (deleteIntersectUpsert.nonEmpty) {
        DBIO.failed(new RawlsException(s"Can't saveEntityPatch on $entityRef because upserts and deletes share attributes $deleteIntersectUpsert"))
      } else {
        getEntityRecords(workspaceContext.workspaceId, Set(entityRef)) flatMap { entityRecs =>
          if (entityRecs.length != 1) {
            throw new RawlsException(s"saveEntityPatch looked up $entityRef expecting 1 record, got ${entityRecs.length} instead")
          }

          val entityRecord = entityRecs.head
          upserts.keys.foreach { attrName =>
            validateUserDefinedString(attrName.name)
            validateAttributeName(attrName, entityRecord.entityType)
          }

          for {
            _ <- applyEntityPatch(workspaceContext, entityRecord, upserts, deletes)
            updatedEntities <- entityQuery.getEntities(Seq(entityRecord.id))
            newRecord = populateAllAttributeValues(entityRecs, updatedEntities.map(elem => elem._2)).head
            _ <- optimisticLockUpdate(newRecord)
          } yield {}
        }
      }
    }

    private def lookupNotYetLoadedReferences(workspaceContext: SlickWorkspaceContext, entities: Traversable[Entity], alreadyLoadedEntityRecs: Seq[EntityRecord]): ReadAction[Seq[EntityRecord]] = {
      val allRefAttributes = (for {
        entity <- entities
        (_, attribute) <- entity.attributes
        ref <- attribute match {
          case AttributeEntityReferenceList(l) => l
          case r: AttributeEntityReference => Seq(r)
          case _ => Seq.empty
        }
      } yield ref).toSet

      lookupNotYetLoadedReferences(workspaceContext, allRefAttributes, alreadyLoadedEntityRecs)
    }

    private def lookupNotYetLoadedReferences(workspaceContext: SlickWorkspaceContext, attrReferences: Set[AttributeEntityReference], alreadyLoadedEntityRecs: Seq[EntityRecord]): ReadAction[Seq[EntityRecord]] = {
      val notYetLoadedEntityRecs = attrReferences -- alreadyLoadedEntityRecs.map(_.toReference)

      getEntityRecords(workspaceContext.workspaceId, notYetLoadedEntityRecs) map { foundEntities =>
        if (foundEntities.size != notYetLoadedEntityRecs.size) {
          val notFoundRefs = notYetLoadedEntityRecs -- foundEntities.map(_.toReference)
          throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Could not resolve some entity references", notFoundRefs.map { missingRef =>
            ErrorReport(s"${missingRef.entityType} ${missingRef.entityName} not found", Seq.empty)
          }.toSeq))
        } else {
          foundEntities
        }
      }
    }

    private def optimisticLockUpdate(originalRec: EntityRecord): ReadWriteAction[Int] = {
      findEntityByIdAndVersion(originalRec.id, originalRec.recordVersion) update originalRec.copy(recordVersion = originalRec.recordVersion + 1) map {
        case 0 => throw new RawlsConcurrentModificationException(s"could not update $originalRec because its record version has changed")
        case success => success
      }
    }

    private def rewriteAttributes(entitiesToSave: Traversable[Entity], entityIds: Seq[Long], entityIdsByRef: Map[AttributeEntityReference, Long]): ReadWriteAction[Int] = {
      val attributesToSave = for {
        entity <- entitiesToSave
        (attributeName, attribute) <- entity.attributes
        attributeRec <- entityAttributeQuery.marshalAttribute(entityIdsByRef(entity.toReference), attributeName, attribute, entityIdsByRef)
      } yield attributeRec

      entityAttributeQuery.findByOwnerQuery(entityIds).result flatMap { existingAttributes =>
        entityAttributeQuery.rewriteAttrsAction(attributesToSave, existingAttributes, entityAttributeScratchQuery.insertScratchAttributes)
      }
    }

    private def insertNewEntities(workspaceContext: SlickWorkspaceContext, entities: Traversable[Entity], preExistingEntityRecs: Seq[EntityRecord]): ReadWriteAction[Seq[EntityRecord]] = {
      val existingEntityRefs = preExistingEntityRecs.map(_.toReference)
      val newEntities = entities.filterNot { e => existingEntityRefs.contains(e.toReference) }

      val newEntityRecs = newEntities.map(e => marshalNewEntity(e, workspaceContext.workspaceId))
      batchInsertEntities(workspaceContext, newEntityRecs.toSeq)
    }

    private def batchInsertEntities(workspaceContext: SlickWorkspaceContext, entities: Seq[EntityRecord]): ReadWriteAction[Seq[EntityRecord]] = {
      if(entities.nonEmpty) {
        workspaceQuery.updateLastModified(workspaceContext.workspaceId) andThen
          insertInBatches(entityQuery, entities) andThen getEntityRecords(workspaceContext.workspaceId, entities.map(_.toReference).toSet)
      }
      else {
        DBIO.successful(Seq.empty[EntityRecord])
      }
    }

    // "delete" entities by hiding and renaming

    def hide(workspaceContext: SlickWorkspaceContext, entRefs: Seq[AttributeEntityReference]): ReadWriteAction[Int] = {
      workspaceQuery.updateLastModified(workspaceContext.workspaceId) andThen
        getEntityRecords(workspaceContext.workspaceId, entRefs.toSet) flatMap { entRecs =>
          val hideAction = entRecs map { rec =>
            hideEntityAttributes(rec.id) andThen
              hideEntityAction(rec)
          }
          DBIO.sequence(hideAction).map(_.sum)
        }
    }

    private def hideEntityAttributes(entityId: Long): ReadWriteAction[Seq[Int]] = {
      findActiveAttributesByEntityId(entityId).result flatMap { attrRecs =>
        DBIO.sequence(attrRecs map hideEntityAttribute)
      }
    }

    private def hideEntityAttribute(attrRec: EntityAttributeRecord): WriteAction[Int] = {
      val currentTime = new Timestamp(new Date().getTime)
      entityAttributeQuery.filter(_.id === attrRec.id).map(rec => (rec.deleted, rec.name, rec.deletedDate)).update(true, renameForHiding(attrRec.id, attrRec.name), Option(currentTime))
    }

    private def hideEntityAction(entRec: EntityRecord): WriteAction[Int] = {
      val currentTime = new Timestamp(new Date().getTime)
      findEntityById(entRec.id).map(rec => (rec.deleted, rec.name, rec.deletedDate)).update(true, renameForHiding(entRec.id, entRec.name), Option(currentTime))
    }

    // perform actual deletion (not hiding) of all entities in a workspace

    def deleteFromDb(workspaceId: UUID): WriteAction[Int] = {
      EntityDependenciesDeletionQuery.deleteAction(workspaceId) andThen {
        filter(_.workspaceId === workspaceId).delete
      }
    }

    def rename(workspaceContext: SlickWorkspaceContext, entityType: String, oldName: String, newName: String): ReadWriteAction[Int] = {
      workspaceQuery.updateLastModified(workspaceContext.workspaceId) andThen
        findEntityByName(workspaceContext.workspaceId, entityType, oldName).map(_.name).update(newName)
    }

    // copy all entities from one workspace to another (empty) workspace

    def copyAllEntities(sourceWorkspaceContext: SlickWorkspaceContext, destWorkspaceContext: SlickWorkspaceContext): ReadWriteAction[Int] = {
      listActiveEntities(sourceWorkspaceContext).flatMap(copyEntities(destWorkspaceContext, _, Seq.empty))
    }

    // copy entities from one workspace to another, checking for conflicts first

    def checkAndCopyEntities(sourceWorkspaceContext: SlickWorkspaceContext, destWorkspaceContext: SlickWorkspaceContext, entityType: String, entityNames: Seq[String], linkExistingEntities: Boolean): ReadWriteAction[EntityCopyResponse] = {
      def getHardConflicts(workspaceId: UUID, entityRefs: Seq[AttributeEntityReference]) = {
        val batchActions = createBatches(entityRefs.toSet).map(batch => getEntityRecords(workspaceId, batch))
        DBIO.sequence(batchActions).map(_.flatten.toSeq).map { recs =>
          recs.map(_.toReference)
        }
      }

      def getSoftConflicts(paths: Seq[EntityPath]) = {
        getCopyConflicts(destWorkspaceContext, paths.map(_.path.last)).map { conflicts =>
          val conflictsAsRefs = conflicts.toSeq.map(_.toReference)
          paths.filter(p => conflictsAsRefs.contains(p.path.last))
        }
      }

      def buildSoftConflictTree(pathsRemaining: EntityPath): Seq[EntitySoftConflict] = {
        if(pathsRemaining.path.isEmpty) Seq.empty
        else Seq(EntitySoftConflict(pathsRemaining.path.head.entityType, pathsRemaining.path.head.entityName, buildSoftConflictTree(EntityPath(pathsRemaining.path.tail))))
      }

      val entitiesToCopyRefs = entityNames.map(name => AttributeEntityReference(entityType, name))

      getHardConflicts(destWorkspaceContext.workspaceId, entitiesToCopyRefs).flatMap {
        case Seq() => {
          val pathsAndConflicts = for {
            entityPaths <- getEntitySubtrees(sourceWorkspaceContext, entityType, entityNames.toSet)
            softConflicts <- getSoftConflicts(entityPaths)
          } yield (entityPaths, softConflicts)

          pathsAndConflicts.flatMap { case (entityPaths, softConflicts) =>
            if(softConflicts.isEmpty || linkExistingEntities) {
              val allEntityRefs = entityPaths.flatMap(_.path)
              val allConflictRefs = softConflicts.flatMap(_.path)
              val entitiesToCopy = allEntityRefs diff allConflictRefs

              for {
                entities <- getActiveEntities(sourceWorkspaceContext, entitiesToCopy)
                _ <- copyEntities(destWorkspaceContext, entities.toSet, allConflictRefs)
              } yield EntityCopyResponse(entities.map(_.toReference).toSeq, Seq.empty, Seq.empty)
            } else {
              val unmergedSoftConflicts = softConflicts.flatMap(buildSoftConflictTree).groupBy(c => (c.entityType, c.entityName)).map {
                case ((conflictType, conflictName), conflicts) => EntitySoftConflict(conflictType, conflictName, conflicts.flatMap(_.conflicts))
              }.toSeq
              DBIO.successful(EntityCopyResponse(Seq.empty, Seq.empty, unmergedSoftConflicts))
            }
          }
        }
        case hardConflicts => DBIO.successful(EntityCopyResponse(Seq.empty, hardConflicts.map(c => EntityHardConflict(c.entityType, c.entityName)), Seq.empty))
      }
    }

    private def copyEntities(destWorkspaceContext: SlickWorkspaceContext, entitiesToCopy: TraversableOnce[Entity], entitiesToReference: Seq[AttributeEntityReference]): ReadWriteAction[Int] = {
      batchInsertEntities(destWorkspaceContext, entitiesToCopy.toSeq.map(marshalNewEntity(_, destWorkspaceContext.workspaceId))) flatMap { insertedRecs =>
        getEntityRecords(destWorkspaceContext.workspaceId, entitiesToReference.toSet) flatMap { toReferenceRecs =>
          val idsByRef = (insertedRecs ++ toReferenceRecs).map { rec => rec.toReference -> rec.id }.toMap
          val entityIdAndEntity = entitiesToCopy map { ent => idsByRef(ent.toReference) -> ent }

          val attributes = for {
            (entityId, entity) <- entityIdAndEntity
            (attributeName, attr) <- entity.attributes
            rec <- entityAttributeQuery.marshalAttribute(entityId, attributeName, attr, idsByRef)
          } yield rec

          entityAttributeQuery.batchInsertAttributes(attributes.toSeq)
        }
      }
    }

    // retrieve all paths from these entities, for checkAndCopyEntities

    private[slick] def getEntitySubtrees(workspaceContext: SlickWorkspaceContext, entityType: String, entityNames: Set[String]): ReadAction[Seq[EntityPath]] = {
      val startingEntityRecsAction = filter(rec => rec.workspaceId === workspaceContext.workspaceId && rec.entityType === entityType && rec.name.inSetBind(entityNames))

      startingEntityRecsAction.result.flatMap { startingEntityRecs =>
        val refsToId = startingEntityRecs.map(rec => EntityPath(Seq(rec.toReference)) -> rec.id).toMap
        recursiveGetEntityReferences(Down, startingEntityRecs.map(_.id).toSet, refsToId)
      }
    }

    // return the entities already present in the destination workspace

    private[slick] def getCopyConflicts(destWorkspaceContext: SlickWorkspaceContext, entitiesToCopy: TraversableOnce[AttributeEntityReference]): ReadAction[TraversableOnce[EntityRecord]] = {
      getEntityRecords(destWorkspaceContext.workspaceId, entitiesToCopy.toSet)
    }

    // the opposite of getEntitySubtrees: traverse the graph to retrieve all entities which ultimately refer to these

    def getAllReferringEntities(context: SlickWorkspaceContext, entities: Set[AttributeEntityReference]): ReadAction[Set[AttributeEntityReference]] = {
      getEntityRecords(context.workspaceId, entities) flatMap { entityRecs =>
        val refsToId = entityRecs.map(rec => EntityPath(Seq(rec.toReference)) -> rec.id).toMap
        recursiveGetEntityReferences(Up, entityRecs.map(_.id).toSet, refsToId)
      } flatMap { refs =>
        val entityAction = EntityAndAttributesRawSqlQuery.activeActionForRefs(context, refs.flatMap(_.path).toSet) map unmarshalEntities
        entityAction map { _.toSet map { e: Entity => e.toReference } }
      }
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
    private def recursiveGetEntityReferences(direction: RecursionDirection, entityIds: Set[Long], accumulatedPathsWithLastId: Map[EntityPath, Long]): ReadAction[Seq[EntityPath]] = {
      def oneLevelDown(idBatch: Set[Long]): ReadAction[Set[(Long, EntityRecord)]] = {
        val query = entityAttributeQuery filter (_.ownerId inSetBind idBatch) join
          this on { (attr, ent) => attr.valueEntityRef === ent.id && ! attr.deleted } map { case (attr, entity) => (attr.ownerId, entity)}
        query.result.map(_.toSet)
      }

      def oneLevelUp(idBatch: Set[Long]): ReadAction[Set[(Long, EntityRecord)]] = {
        val query = entityAttributeQuery filter (_.valueEntityRef inSetBind idBatch) join
          this on { (attr, ent) => attr.ownerId === ent.id && ! ent.deleted } map { case (attr, entity) => (attr.valueEntityRef.get, entity)}
        query.result.map(_.toSet)
      }

      // need to batch because some RDBMSes have a limit on the length of an in clause
      val batchedEntityIds: Iterable[Set[Long]] = createBatches(entityIds)

      val batchActions: Iterable[ReadAction[Set[(Long, EntityRecord)]]] = direction match {
        case Down => batchedEntityIds map oneLevelDown
        case Up => batchedEntityIds map oneLevelUp
      }

      DBIO.sequence(batchActions).map(_.flatten.toSet).flatMap { priorIdWithCurrentRec =>
        val currentPaths = priorIdWithCurrentRec.flatMap { case (priorId, currentRec) =>
          val pathsThatEndWithPrior = accumulatedPathsWithLastId.filter { case (path, id) => id == priorId }
          pathsThatEndWithPrior.keys.map(_.path).map { path => (EntityPath(path :+ currentRec.toReference), currentRec.id)}
        }.toMap

        val untraversedIds = priorIdWithCurrentRec.map(_._2.id) -- accumulatedPathsWithLastId.values
        if (untraversedIds.isEmpty) {
          DBIO.successful(accumulatedPathsWithLastId.keys.toSeq)
        } else {
          recursiveGetEntityReferences(direction, untraversedIds, accumulatedPathsWithLastId ++ currentPaths)
        }
      }
    }

    // Utility methods

    private def createAllAttributesString(entity: Entity): Option[String] = {
      Option(s"${entity.name} ${entity.attributes.values.filterNot(_.isInstanceOf[AttributeList[_]]).map(AttributeStringifier(_)).mkString(" ")}".toLowerCase)
    }

    private def populateAllAttributeValues(entityRecsFromDb: Seq[EntityRecord], entitiesToSave: Traversable[Entity]): Seq[EntityRecord] = {
      val entitiesByRef = entitiesToSave.map(e => e.toReference -> e).toMap
      entityRecsFromDb.map { rec =>
        rec.copy(allAttributeValues = createAllAttributesString(entitiesByRef(rec.toReference)))
      }
    }

    private def validateEntity(entity: Entity): Unit = {
      if (entity.entityType.equalsIgnoreCase(Attributable.workspaceEntityType)) {
        throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(message = s"Entity type ${Attributable.workspaceEntityType} is reserved", statusCode = StatusCodes.BadRequest))
      }
      validateUserDefinedString(entity.entityType)
      validateUserDefinedString(entity.name)
      entity.attributes.keys.foreach { attrName =>
        validateUserDefinedString(attrName.name)
        validateAttributeName(attrName, entity.entityType)
      }
    }

    // Marshal/Unmarshal methods

    private def marshalNewEntity(entity: Entity, workspaceId: UUID): EntityRecord = {
      EntityRecord(0, entity.name, entity.entityType, workspaceId, 0, createAllAttributesString(entity), deleted = false, deletedDate = None)
    }

    private def unmarshalEntity(entityRecord: EntityRecord, attributes: AttributeMap): Entity = {
      Entity(entityRecord.name, entityRecord.entityType, attributes)
    }

    private def unmarshalEntities(entityAttributeRecords: Seq[entityQuery.EntityAndAttributesRawSqlQuery.EntityAndAttributesResult]): Seq[Entity] = {
      unmarshalEntitiesWithIds(entityAttributeRecords).map { case (id, entity) => entity }
    }

    private def unmarshalEntitiesWithIds(entityAttributeRecords: Seq[entityQuery.EntityAndAttributesRawSqlQuery.EntityAndAttributesResult]): Seq[(Long, Entity)] = {
      val allEntityRecords = entityAttributeRecords.map(_.entityRecord).distinct

      // note that not all entities have attributes, thus the collect below
      val entitiesWithAttributes = entityAttributeRecords.collect {
        case EntityAndAttributesRawSqlQuery.EntityAndAttributesResult(entityRec, Some(attributeRec), refEntityRecOption) => ((entityRec.id, attributeRec), refEntityRecOption)
      }

      val attributesByEntityId = entityAttributeQuery.unmarshalAttributes(entitiesWithAttributes)

      allEntityRecords.map { entityRec =>
        entityRec.id -> unmarshalEntity(entityRec, attributesByEntityId.getOrElse(entityRec.id, Map.empty))
      }
    }
  }
}
