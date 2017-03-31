package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.util.UUID

import org.broadinstitute.dsde.rawls.util.CollectionUtils
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, model}
import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model._
import slick.driver.JdbcDriver
import slick.jdbc.GetResult
import spray.http.StatusCodes

/**
 * Created by dvoet on 2/4/16.
 */
case class EntityRecord(id: Long, name: String, entityType: String, workspaceId: UUID, recordVersion: Long, allAttributeValues: Option[String], deleted: Boolean) {
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

    def workspace = foreignKey("FK_ENTITY_WORKSPACE", workspaceId, workspaceQuery)(_.id)
    def uniqueTypeName = index("idx_entity_type_name", (workspaceId, entityType, name), unique = true)

    def * = (id, name, entityType, workspaceId, version, allAttributeValues, deleted) <> (EntityRecord.tupled, EntityRecord.unapply)
  }

  object entityQuery extends TableQuery(new EntityTable(_)) {

    type EntityQuery = Query[EntityTable, EntityRecord, Seq]
    type EntityAttributeQuery = Query[EntityAttributeTable, EntityAttributeRecord, Seq]
    type EntityQueryWithAttributesAndRefs =  Query[(EntityTable, Rep[Option[(EntityAttributeTable, Rep[Option[EntityTable]])]]), (EntityRecord, Option[(EntityAttributeRecord, Option[EntityRecord])]), Seq]

    private object EntityRecordRawSqlQuery extends RawSqlQuery {
      val driver: JdbcDriver = EntityComponent.this.driver
      implicit val getEntityRecord = GetResult { r => EntityRecord(r.<<, r.<<, r.<<, r.<<, r.<<, None, r.<<) }

      def action(workspaceId: UUID, entities: Traversable[AttributeEntityReference]): ReadAction[Seq[EntityRecord]] = {
        val baseSelect = sql"select id, name, entity_type, workspace_id, record_version, deleted from ENTITY where workspace_id = $workspaceId and (entity_type, name) in ("
        val entityTypeNameTuples = reduceSqlActionsWithDelim(entities.map { entity => sql"(${entity.entityType}, ${entity.entityName})" }.toSeq)
        concatSqlActions(baseSelect, entityTypeNameTuples, sql")").as[EntityRecord]
      }
    }

    private object EntityAndAttributesRawSqlQuery extends RawSqlQuery {
      val driver: JdbcDriver = EntityComponent.this.driver

      // result structure from entity and attribute list raw sql
      case class EntityAndAttributesResult(entityRecord: EntityRecord, attributeRecord: Option[EntityAttributeRecord], refEntityRecord: Option[EntityRecord])

      // tells slick how to convert a result row from a raw sql query to an instance of EntityAndAttributesResult
      implicit val getEntityAndAttributesResult = GetResult { r =>
        // note that the number and order of all the r.<< match precisely with the select clause of baseEntityAndAttributeSql
        val entityRec = EntityRecord(r.<<, r.<<, r.<<, r.<<, r.<<, None, r.<<)

        val attributeIdOption: Option[Long] = r.<<
        val attributeRecOption = attributeIdOption.map(id => EntityAttributeRecord(id, entityRec.id, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

        val refEntityRecOption = for {
          attributeRec <- attributeRecOption
          refId <- attributeRec.valueEntityRef
        } yield {
          EntityRecord(r.<<, r.<<, r.<<, r.<<, r.<<, None, r.<<)
        }

        EntityAndAttributesResult(entityRec, attributeRecOption, refEntityRecOption)
      }

      // the where clause for this query is filled in specific to the use case
      val baseEntityAndAttributeSql =
        s"""select e.id, e.name, e.entity_type, e.workspace_id, e.record_version, e.deleted,
          a.id, a.namespace, a.name, a.value_string, a.value_number, a.value_boolean, a.value_json, a.value_entity_ref, a.list_index, a.list_length, a.deleted,
          e_ref.id, e_ref.name, e_ref.entity_type, e_ref.workspace_id, e_ref.record_version, e_ref.deleted
          from ENTITY e
          left outer join ENTITY_ATTRIBUTE a on a.owner_id = e.id and a.deleted = e.deleted
          left outer join ENTITY e_ref on a.value_entity_ref = e_ref.id"""

      // includes "deleted" hidden entities when using their hidden names
      def actionForTypeName(workspaceContext: SlickWorkspaceContext, entityType: String, entityName: String): ReadAction[Seq[EntityAndAttributesResult]] = {
        sql"""#$baseEntityAndAttributeSql where e.name = ${entityName} and e.entity_type = ${entityType} and e.workspace_id = ${workspaceContext.workspaceId}""".as[EntityAndAttributesResult]
      }

      def actionForType(workspaceContext: SlickWorkspaceContext, entityType: String): ReadAction[Seq[EntityAndAttributesResult]] = {
        sql"""#$baseEntityAndAttributeSql where e.deleted = false and e.entity_type = ${entityType} and e.workspace_id = ${workspaceContext.workspaceId}""".as[EntityAndAttributesResult]
      }

      // includes "deleted" hidden entities when using their hidden names
      def actionForRefs(workspaceContext: SlickWorkspaceContext, entityRefs: Traversable[AttributeEntityReference]): ReadAction[Seq[EntityAndAttributesResult]] = {
        val baseSelect = sql"""#$baseEntityAndAttributeSql where e.deleted = false and e.workspace_id = ${workspaceContext.workspaceId} and (e.entity_type, e.name) in ("""
        val entityTypeNameTuples = reduceSqlActionsWithDelim(entityRefs.map { ref => sql"(${ref.entityType}, ${ref.entityName})" }.toSeq)
        concatSqlActions(baseSelect, entityTypeNameTuples, sql")").as[EntityAndAttributesResult]
      }

      // includes "deleted" hidden entities
      def actionForIds(entityIds: Traversable[Long]): ReadAction[Seq[EntityAndAttributesResult]] = {
        val baseSelect = sql"""#$baseEntityAndAttributeSql where e.id in ("""
        val entityIdSql = reduceSqlActionsWithDelim(entityIds.map { id => sql"$id" }.toSeq)
        concatSqlActions(baseSelect, entityIdSql, sql")").as[EntityAndAttributesResult]
      }

      def activeActionForWorkspace(workspaceContext: SlickWorkspaceContext): ReadAction[Seq[EntityAndAttributesResult]] = {
        sql"""#$baseEntityAndAttributeSql where e.deleted = false and e.workspace_id = ${workspaceContext.workspaceId}""".as[EntityAndAttributesResult]
      }

      // includes "deleted" hidden entities
      def actionForWorkspace(workspaceContext: SlickWorkspaceContext): ReadAction[Seq[EntityAndAttributesResult]] = {
        sql"""#$baseEntityAndAttributeSql where e.workspace_id = ${workspaceContext.workspaceId}""".as[EntityAndAttributesResult]
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

      def actionForPagination(workspaceContext: SlickWorkspaceContext, entityType: String, entityQuery: model.EntityQuery) = {
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
          unfilteredCount <- findEntityByType(workspaceContext.workspaceId, entityType).length.result
          page <- concatSqlActions(sql"#$baseEntityAndAttributeSql", paginationJoin, order("p")).as[EntityAndAttributesResult]
        } yield (unfilteredCount, filteredCount.head, page)
      }
    }

    def loadEntityPage(workspaceContext: SlickWorkspaceContext, entityType: String, entityQuery: model.EntityQuery): ReadAction[(Int, Int, Iterable[Entity])] = {
      EntityAndAttributesRawSqlQuery.actionForPagination(workspaceContext, entityType, entityQuery) map { case (unfilteredCount, filteredCount, pagination) =>
        (unfilteredCount, filteredCount, unmarshalEntitiesWithIds(pagination).map { case (id, entity) => entity })
      }
    }

    def entityAttributes(entityId: Rep[Long]): EntityAttributeQuery = for {
      entityAttrRec <- entityAttributeQuery if entityAttrRec.ownerId === entityId && ! entityAttrRec.deleted
    } yield entityAttrRec

    // includes "deleted" hidden entities when using their hidden names
    def findEntityByName(workspaceId: UUID, entityType: String, entityName: String): EntityQuery = {
      filter(entRec => entRec.name === entityName && entRec.entityType === entityType && entRec.workspaceId === workspaceId)
    }

    def findEntityByType(workspaceId: UUID, entityType: String): EntityQuery = {
      filter(entRec => entRec.entityType === entityType && entRec.workspaceId === workspaceId && ! entRec.deleted)
    }

    def findEntityByWorkspace(workspaceId: UUID): EntityQuery = {
      filter(entRec => entRec.workspaceId === workspaceId && ! entRec.deleted)
    }

    // includes "deleted" hidden entities
    def findEntityById(id: Long): EntityQuery = {
      filter(_.id === id)
    }

    def findEntityByIdAndVersion(id: Long, version: Long): EntityQuery = {
      filter(rec => rec.id === id && rec.version === version)
    }

    // includes "deleted" hidden entities when using their hidden names
    def lookupEntitiesByNames(workspaceId: UUID, entities: Traversable[AttributeEntityReference]): ReadAction[Seq[EntityRecord]] = {
      if (entities.isEmpty) {
        DBIO.successful(Seq.empty)
      } else {
        // slick can't do a query with '(entityType, entityName) in ((?, ?), (?, ?), ...)' so we need raw sql
        EntityRecordRawSqlQuery.action(workspaceId, entities)
      }
    }

    // includes "deleted" hidden entities when using their hidden names
    def get(workspaceContext: SlickWorkspaceContext, entityType: String, entityName: String): ReadAction[Option[Entity]] = {
      unmarshalEntities(EntityAndAttributesRawSqlQuery.actionForTypeName(workspaceContext, entityType, entityName)).map(_.headOption)
    }

    /**
     * converts a query resulting in a number of records representing many entities with many attributes, some of them references
     */
    def unmarshalEntities(entityAndAttributesQuery: EntityQueryWithAttributesAndRefs): ReadAction[Iterable[Entity]] = {
      entityAndAttributesQuery.result map { entityAttributeRecords =>
        val attributeRecords = entityAttributeRecords.collect { case (entityRec, Some((attributeRec, referenceOption))) => ((entityRec.id, attributeRec), referenceOption) }
        val attributesByEntityId = entityAttributeQuery.unmarshalAttributes(attributeRecords)

        entityAttributeRecords.map { case (entityRec, _) =>
          unmarshalEntity(entityRec, attributesByEntityId.getOrElse(entityRec.id, Map.empty))
        }
      }
    }

    def unmarshalEntities(entityAttributeAction: ReadAction[Seq[EntityAndAttributesRawSqlQuery.EntityAndAttributesResult]]): ReadAction[Iterable[Entity]] = {
      unmarshalEntitiesWithIds(entityAttributeAction).map(_.map { case (id, entity) => entity })
    }

    def unmarshalEntitiesWithIds(entityAttributeAction: ReadAction[Seq[EntityAndAttributesRawSqlQuery.EntityAndAttributesResult]]): ReadAction[Seq[(Long, Entity)]] = {
      entityAttributeAction.map(unmarshalEntitiesWithIds)
    }

    def unmarshalEntitiesWithIds(entityAttributeRecords: Seq[entityQuery.EntityAndAttributesRawSqlQuery.EntityAndAttributesResult]): Seq[(Long, Entity)] = {
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

    /** creates or replaces an entity */
    def save(workspaceContext: SlickWorkspaceContext, entity: Entity): ReadWriteAction[Entity] = {
      save(workspaceContext, Seq(entity)).map(_.head)
    }

    def save(workspaceContext: SlickWorkspaceContext, entities: Traversable[Entity]): ReadWriteAction[Traversable[Entity]] = {
      workspaceQuery.updateLastModified(workspaceContext.workspaceId)
      entities.foreach(validateEntity)

      for {
        preExistingEntityRecs <- lookupEntitiesByNames(workspaceContext.workspaceId, entities.map(_.toReference)).map(populateAllAttributeValues(_, entities))
        savingEntityRecs <- insertNewEntities(workspaceContext, entities, preExistingEntityRecs).map(_ ++ preExistingEntityRecs)
        referencedAndSavingEntityRecs <- lookupNotYetLoadedReferences(workspaceContext, entities, savingEntityRecs).map(_ ++ savingEntityRecs)
        _ <- upsertAttributes(entities, savingEntityRecs.map(_.id), referencedAndSavingEntityRecs.map(e => e.toReference -> e.id).toMap)
        _ <- DBIO.seq(preExistingEntityRecs map optimisticLockUpdate: _ *)
      } yield entities
    }

    def populateAllAttributeValues(entityRecsFromDb: Seq[EntityRecord], entitiesToSave: Traversable[Entity]): Seq[EntityRecord] = {
      val entitiesByRef = entitiesToSave.map(e => e.toReference -> e).toMap
      entityRecsFromDb.map { rec =>
        rec.copy(allAttributeValues = createAllAttributesString(entitiesByRef(rec.toReference)))
      }
    }

    private def optimisticLockUpdate(originalRec: EntityRecord): ReadWriteAction[Int] = {
      findEntityByIdAndVersion(originalRec.id, originalRec.recordVersion) update originalRec.copy(recordVersion = originalRec.recordVersion + 1) map {
        case 0 => throw new RawlsConcurrentModificationException(s"could not update $originalRec because its record version has changed")
        case success => success
      }
    }

    private def upsertAttributes(entitiesToSave: Traversable[Entity], entityIds: Seq[Long], entityIdsByRef: Map[AttributeEntityReference, Long]) = {
      val attributesToSave = for {
        entity <- entitiesToSave
        (attributeName, attribute) <- entity.attributes
        attributeRec <- entityAttributeQuery.marshalAttribute(entityIdsByRef(entity.toReference), attributeName, attribute, entityIdsByRef)
      } yield attributeRec

      def insertScratchAttributes(attributeRecs: Seq[EntityAttributeRecord])(transactionId: String): WriteAction[Int] = {
        entityAttributeScratchQuery.batchInsertAttributes(attributeRecs, transactionId)
      }

      entityAttributeQuery.findByOwnerQuery(entityIds).result flatMap { existingAttributes =>
        entityAttributeQuery.upsertAction(attributesToSave, existingAttributes, insertScratchAttributes)
      }
    }

    private def lookupNotYetLoadedReferences(workspaceContext: SlickWorkspaceContext, entities: Traversable[Entity], alreadyLoadedEntityRecs: Seq[EntityRecord]): ReadAction[Seq[EntityRecord]] = {
      val notYetLoadedEntityRecs = (for {
        entity <- entities
        (_, attribute) <- entity.attributes
        ref <- attribute match {
          case AttributeEntityReferenceList(l) => l
          case r: AttributeEntityReference => Seq(r)
          case _ => Seq.empty
        }
      } yield ref).toSet -- alreadyLoadedEntityRecs.map(_.toReference)

      lookupEntitiesByNames(workspaceContext.workspaceId, notYetLoadedEntityRecs) map { foundEntities =>
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

    private def insertNewEntities(workspaceContext: SlickWorkspaceContext, entities: Traversable[Entity], preExistingEntityRecs: Seq[EntityRecord]): ReadWriteAction[Seq[EntityRecord]] = {
      val existingEntityTypeNames = preExistingEntityRecs.map(rec => (rec.entityType, rec.name))
      val newEntities = entities.filterNot(e => existingEntityTypeNames.exists(_ ==(e.entityType, e.name)))

      val newEntityRecs = newEntities.map(e => marshalNewEntity(e, workspaceContext.workspaceId))
      batchInsertEntities(workspaceContext, newEntityRecs.toSeq)
    }

    /** "deletes" entities by hiding and renaming */
    def hide(workspaceContext: SlickWorkspaceContext, entRefs: Seq[AttributeEntityReference]): ReadWriteAction[Int] = {
      workspaceQuery.updateLastModified(workspaceContext.workspaceId) andThen
        lookupEntitiesByNames(workspaceContext.workspaceId, entRefs) flatMap { entRecs =>
          val hideAction = entRecs map { rec =>
            hideEntityAttributes(rec.id) andThen
              hideEntityAction(rec)
          }
          DBIO.sequence(hideAction).map(_.sum)
        }
    }

    private def hideEntityAttributes(entityId: Long): ReadWriteAction[Seq[Int]] = {
      entityAttributes(entityId).result flatMap { attrRecs =>
        DBIO.sequence(attrRecs map hideEntityAttribute)
      }
    }

    private def hideEntityAttribute(attrRec: EntityAttributeRecord): WriteAction[Int] = {
      entityAttributeQuery.filter(_.id === attrRec.id).map(rec => (rec.deleted, rec.name)).update(true, renameForHiding(attrRec.name))
    }

    private def hideEntityAction(entRec: EntityRecord): WriteAction[Int] = {
      entityQuery.filter(_.id === entRec.id).map(rec => (rec.deleted, rec.name)).update(true, renameForHiding(entRec.name))
    }

    // performs actual deletion (not hiding) of everything that depends on an entity
    object EntityDependenciesDeletionQuery extends RawSqlQuery {
      val driver: JdbcDriver = EntityComponent.this.driver

      def deleteAction(workspaceId: UUID): WriteAction[Int] =
        sqlu"""delete ea from ENTITY_ATTRIBUTE ea
               inner join ENTITY e
               on ea.owner_id = e.id
               where e.workspace_id=${workspaceId}
          """
    }

    // performs actual deletion (not hiding) of an entity
    def deleteFromDb(workspaceId: UUID): WriteAction[Int] = {
      EntityDependenciesDeletionQuery.deleteAction(workspaceId) andThen {
        filter(_.workspaceId === workspaceId).delete
      }
    }

    /** list all entities of the given type in the workspace */
    def list(workspaceContext: SlickWorkspaceContext, entityType: String): ReadAction[TraversableOnce[Entity]] = {
      unmarshalEntities(EntityAndAttributesRawSqlQuery.actionForType(workspaceContext, entityType))
    }

    // includes "deleted" hidden entities when using their hidden names
    def list(workspaceContext: SlickWorkspaceContext, entityRefs: Traversable[AttributeEntityReference]): ReadAction[TraversableOnce[Entity]] = {
      unmarshalEntities(EntityAndAttributesRawSqlQuery.actionForRefs(workspaceContext, entityRefs))
    }

    // includes "deleted" hidden entities
    def listByIds(entityIds: Traversable[Long]): ReadAction[Seq[(Long, Entity)]] = {
      unmarshalEntitiesWithIds(EntityAndAttributesRawSqlQuery.actionForIds(entityIds))
    }

    def rename(workspaceContext: SlickWorkspaceContext, entityType: String, oldName: String, newName: String): ReadWriteAction[Int] = {
      workspaceQuery.updateLastModified(workspaceContext.workspaceId) andThen
        findEntityByName(workspaceContext.workspaceId, entityType, oldName).map(_.name).update(newName)
    }

    def getEntityTypesWithCounts(workspaceContext: SlickWorkspaceContext): ReadAction[Map[String, Int]] = {
      filter(e => e.workspaceId === workspaceContext.workspaceId && ! e.deleted).groupBy(e => e.entityType).map { case (entityType, entities) =>
        (entityType, entities.length)
      }.result map { result =>
        result.toMap
      }
    }

    def getAttrNamesAndEntityTypes(workspaceContext: SlickWorkspaceContext): ReadAction[Map[String, Seq[String]]] = {
      val typesAndAttrNames = for {
        entityRec <- filter(e => e.workspaceId === workspaceContext.workspaceId && ! e.deleted)
        attrib <- entityAttributes(entityRec.id)
      } yield {
        (entityRec.entityType, attrib.name)
      }

      typesAndAttrNames.distinct.result map { result =>
        CollectionUtils.groupByTuples(result)
      }
    }

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

    def listActiveEntitiesAllTypes(workspaceContext: SlickWorkspaceContext): ReadAction[TraversableOnce[Entity]] = {
      unmarshalEntities(EntityAndAttributesRawSqlQuery.activeActionForWorkspace(workspaceContext))
    }

    // includes "deleted" hidden entities
    def listEntitiesAllTypes(workspaceContext: SlickWorkspaceContext): ReadAction[TraversableOnce[Entity]] = {
      unmarshalEntities(EntityAndAttributesRawSqlQuery.actionForWorkspace(workspaceContext))
    }

    def cloneAllEntities(sourceWorkspaceContext: SlickWorkspaceContext, destWorkspaceContext: SlickWorkspaceContext): ReadWriteAction[Int] = {
      val allEntitiesAction = listActiveEntitiesAllTypes(sourceWorkspaceContext)

      allEntitiesAction.flatMap(cloneEntities(destWorkspaceContext, _, Seq.empty))
    }

    def batchInsertEntities(workspaceContext: SlickWorkspaceContext, entities: Seq[EntityRecord]): ReadWriteAction[Seq[EntityRecord]] = {
      if(!entities.isEmpty) {
        workspaceQuery.updateLastModified(workspaceContext.workspaceId) andThen
          insertInBatches(entityQuery, entities) andThen selectEntityIds(workspaceContext, entities.map(_.toReference))
      }
      else {
        DBIO.successful(Seq.empty[EntityRecord])
      }
    }

    def selectEntityIds(workspaceContext: SlickWorkspaceContext, entities: Seq[AttributeEntityReference]): ReadAction[Seq[EntityRecord]] = {
      val entitiesGrouped = entities.grouped(batchSize).toSeq

      DBIO.sequence(entitiesGrouped map { batch =>
        EntityRecordRawSqlQuery.action(workspaceContext.workspaceId, batch)
      }).map(_.flatten)
    }

    def cloneEntities(destWorkspaceContext: SlickWorkspaceContext, entitiesToClone: TraversableOnce[Entity], entitiesToReference: Seq[AttributeEntityReference]): ReadWriteAction[Int] = {
      batchInsertEntities(destWorkspaceContext, entitiesToClone.toSeq.map(marshalNewEntity(_, destWorkspaceContext.workspaceId))) flatMap { ids =>
        val entityIdByEntity = ids.map(record => record.id -> entitiesToClone.filter(p => p.entityType == record.entityType && p.name == record.name).toSeq.head)
        val entityIdsByRef = entityIdByEntity.map { case (entityId, entity) => entity.toReference -> entityId }.toMap

        val entitiesToReferenceById = selectEntityIds(destWorkspaceContext, entitiesToReference)

        val attributeRecords = entitiesToReferenceById map { entityRecsToReference =>
          val entityIdsToReferenceById = entityRecsToReference.map(record => record.id -> entitiesToReference.filter(p => p.entityType == record.entityType && p.entityName == record.name).head)
          val entityIdsToReferenceByRef = entityIdsToReferenceById.map { case (entityId, entity) => entity -> entityId }.toMap
          entityIdByEntity flatMap { case (entityId, entity) =>
            entity.attributes.flatMap { case (attributeName, attr) =>
              entityAttributeQuery.marshalAttribute(entityId, attributeName, attr, (entityIdsByRef ++ entityIdsToReferenceByRef))
            }
          }
        }

        attributeRecords flatMap { attributes =>
          entityAttributeQuery.batchInsertAttributes(attributes)
        }
      }
    }

    sealed trait RecursionDirection
    case object Up extends RecursionDirection
    case object Down extends RecursionDirection

    /**
     * Starting with entities specified by entityIds, recursively walk references accumulating all the ids
     * @param direction whether to walk Down (my object references others) or Up (my object is referenced by others) the graph
     * @param entityIds the ids to start with
     * @param accumulatedIds the ids accumulated from the prior call. If you wish entityIds to be in the overall
     *                       results, start with entityIds == accumulatedIds, otherwise start with Seq.empty but note
     *                       that if there is a cycle some of entityIds may be in the result anyway
     * @return the ids of all the entities referred to by entityIds
     */
    private def recursiveGetEntityReferenceIds(direction: RecursionDirection, entityIds: Set[Long], accumulatedIds: Set[Long]): ReadAction[Set[Long]] = {
      def oneLevelDown(idBatch: Set[Long]): ReadAction[Set[Long]] = {
        val query = filter(_.id inSetBind idBatch) join
          entityAttributeQuery on { (ent, attr) => ent.id === attr.ownerId && ! attr.deleted } filter (_._2.valueEntityRef.isDefined) map (_._2.valueEntityRef.get)
        query.result.map(_.toSet)
      }

      def oneLevelUp(idBatch: Set[Long]): ReadAction[Set[Long]] = {
        val query = entityAttributeQuery filter (_.valueEntityRef.isDefined) filter (_.valueEntityRef inSetBind idBatch) join
          this on { (attr, ent) => attr.ownerId === ent.id && ! ent.deleted } map (_._2.id)
        query.result.map(_.toSet)
      }

      // need to batch because some RDBMSes have a limit on the length of an in clause
      val batchedEntityIds: Iterable[Set[Long]] = createBatches(entityIds)

      val batchActions: Iterable[ReadAction[Set[Long]]] = direction match {
        case Down => batchedEntityIds map oneLevelDown
        case Up => batchedEntityIds map oneLevelUp
      }

      DBIO.sequence(batchActions).map(_.flatten.toSet).flatMap { refIds =>
        val untraversedIds = refIds -- accumulatedIds
        if (untraversedIds.isEmpty) {
          DBIO.successful(accumulatedIds)
        } else {
          recursiveGetEntityReferenceIds(direction, untraversedIds, accumulatedIds ++ refIds)
        }
      }
    }

    /**
     * used in copyEntities load all the entities to copy
     */
    def getEntitySubtrees(workspaceContext: SlickWorkspaceContext, entityType: String, entityNames: Seq[String]): ReadAction[TraversableOnce[Entity]] = {
      val startingEntityIdsAction = filter(rec => rec.workspaceId === workspaceContext.workspaceId && rec.entityType === entityType && rec.name.inSetBind(entityNames)).map(_.id)

      startingEntityIdsAction.result.flatMap { startingEntityIds =>
        val idSet = startingEntityIds.toSet
        recursiveGetEntityReferenceIds(Down, idSet, idSet)
      } flatMap { ids =>
        unmarshalEntities(EntityAndAttributesRawSqlQuery.actionForIds(ids))
      }
    }

    case class ConflictSubtree(entity: Entity, conflicts: Seq[Entity], allChildren: Seq[Entity]) {
      def hasConflicts = conflicts.nonEmpty
    }

    def copyEntities(sourceWorkspaceContext: SlickWorkspaceContext, destWorkspaceContext: SlickWorkspaceContext, entityType: String, entityNames: Seq[String], linkExistingEntities: Boolean): ReadWriteAction[EntityCopyResponse] = {

      def getAllAttrRefs(attrs: AttributeMap): Seq[AttributeEntityReference] = {
        attrs.values.collect { case ref: AttributeEntityReference => ref }.toSeq
      }

      def getHardConflicts(workspaceId: UUID, entityRefs: Seq[AttributeEntityReference]) = {
        val batchActions = createBatches(entityRefs.toSet).map(batch => lookupEntitiesByNames(workspaceId, batch))
        DBIO.sequence(batchActions).map(_.flatten.toSet).map { recs =>
            recs.map(rec => AttributeEntityReference(rec.entityType, rec.name))
        }
      }

      def getSoftConflicts(entityType: String, entityNames: Seq[String]) = {
        DBIO.sequence(entityNames.map { entityName =>
          getEntitySubtrees(sourceWorkspaceContext, entityType, Seq(entityName)).flatMap { entities =>
            getCopyConflicts(destWorkspaceContext, entities).flatMap { conflicts =>
              for {
                rootEntity <- get(sourceWorkspaceContext, entityType, entityName)
              } yield ConflictSubtree(rootEntity.get, conflicts.toSeq, entities.toSeq)
            }
          }
        })
      }

      def buildConflictSubtree(topLevelEntityRefs: Map[Entity, Seq[AttributeEntityReference]], subtree: ConflictSubtree, result: Seq[EntitySoftConflict]): Seq[EntitySoftConflict] = {
        if(topLevelEntityRefs.isEmpty) result
        else {
          topLevelEntityRefs.map { case (entity, refs) =>
            val refsToCareAbout = subtree.conflicts.filter(c => refs.contains(AttributeEntityReference(c.entityType, c.name)))
            val entitiesAndRefsToCareAbout = refsToCareAbout.map(e => e -> getAllAttrRefs(e.attributes)).toMap

            EntitySoftConflict(entity.entityType, entity.name, buildConflictSubtree(entitiesAndRefsToCareAbout, subtree, result))
          }.toSeq
        }
      }

      val entitiesToCopyRefs = entityNames.map(name => AttributeEntityReference(entityType, name))

      getHardConflicts(destWorkspaceContext.workspaceId, entitiesToCopyRefs).flatMap {
        case Seq() => getSoftConflicts(entityType, entityNames).flatMap { softConflicts =>
          val allEntities = softConflicts.flatMap(_.allChildren)
          val allConflicts = softConflicts.flatMap(_.conflicts)

          if(allConflicts.isEmpty || linkExistingEntities) {
            cloneEntities(destWorkspaceContext, allEntities diff allConflicts, allConflicts.map(_.toReference)).map { _ =>
              EntityCopyResponse(Seq.empty, Seq.empty, Seq.empty)
            }
          } else {
            DBIO.successful(EntityCopyResponse(Seq.empty, Seq.empty, softConflicts.flatMap(c => buildConflictSubtree(Map(c.entity -> getAllAttrRefs(c.entity.attributes)), c, Seq.empty))))
          }
        }
        case hardConflicts => DBIO.successful(EntityCopyResponse(Seq.empty, hardConflicts.map(c => EntityHardConflict(c.entityType, c.entityName)), Seq.empty))
      }
    }

    def getCopyConflicts(destWorkspaceContext: SlickWorkspaceContext, entitiesToCopy: TraversableOnce[Entity]): ReadAction[TraversableOnce[Entity]] = {
      val entityQueries = entitiesToCopy.map { entity =>
        findEntityByName(destWorkspaceContext.workspaceId, entity.entityType, entity.name).result.map {
          case Seq() => None
          case _ => Option(entity)
        }
      }
      DBIO.sequence(entityQueries).map(_.toStream.collect { case Some(e) => e })
    }

    // the opposite of getEntitySubtrees: traverse the graph to retrieve all entities which ultimately refer to these

    def getAllReferringEntities(context: SlickWorkspaceContext, entities: Set[AttributeEntityReference]): ReadAction[Set[AttributeEntityReference]] = {
      lookupEntitiesByNames(context.workspaceId, entities) flatMap { entityRecs =>
        val idSet = entityRecs.map(_.id).toSet
        recursiveGetEntityReferenceIds(Up, idSet, idSet)
      } flatMap { ids =>
        val entityAction = unmarshalEntities(EntityAndAttributesRawSqlQuery.actionForIds(ids))
        entityAction map { _.toSet map { e: Entity => e.toReference } }
      }
    }

    def marshalNewEntity(entity: Entity, workspaceId: UUID): EntityRecord = {
      EntityRecord(0, entity.name, entity.entityType, workspaceId, 0, createAllAttributesString(entity), deleted = false)
    }

    def unmarshalEntity(entityRecord: EntityRecord, attributes: AttributeMap) = {
      Entity(entityRecord.name, entityRecord.entityType, attributes)
    }
  }

  def createAllAttributesString(entity: Entity): Option[String] = {
    Option(s"${entity.name} ${entity.attributes.values.filterNot(_.isInstanceOf[AttributeList[_]]).map(AttributeStringifier(_)).mkString(" ")}".toLowerCase)
  }

  def validateEntity(entity: Entity): Unit = {
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

  case class ExprEvalRecord(id: Long, name: String, transactionId: String)
  class ExprEvalScratch(tag: Tag) extends Table[ExprEvalRecord](tag, "EXPREVAL_SCRATCH") {
    def id = column[Long]("id")
    def name = column[String]("name", O.Length(254))
    def transactionId = column[String]("transaction_id")

    //No foreign key constraint here because MySQL won't allow them on temp tables :(
    //def entityId = foreignKey("FK_EXPREVAL_ENTITY", id, entityQuery)(_.id)

    def * = (id, name, transactionId) <> (ExprEvalRecord.tupled, ExprEvalRecord.unapply)
  }
  val exprEvalQuery = TableQuery[ExprEvalScratch]
}
