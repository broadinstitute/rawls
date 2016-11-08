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
case class EntityRecord(id: Long, name: String, entityType: String, workspaceId: UUID, recordVersion: Long, allAttributeValues: Option[String])

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

    def workspace = foreignKey("FK_ENTITY_WORKSPACE", workspaceId, workspaceQuery)(_.id)
    def uniqueTypeName = index("idx_entity_type_name", (workspaceId, entityType, name), unique = true)

    def * = (id, name, entityType, workspaceId, version, allAttributeValues) <> (EntityRecord.tupled, EntityRecord.unapply)
  }

  object entityQuery extends TableQuery(new EntityTable(_)) {
    type EntityQuery = Query[EntityTable, EntityRecord, Seq]
    type EntityAttributeQuery = Query[EntityAttributeTable, EntityAttributeRecord, Seq]
    type EntityQueryWithAttributesAndRefs =  Query[(EntityTable, Rep[Option[(EntityAttributeTable, Rep[Option[EntityTable]])]]), (EntityRecord, Option[(EntityAttributeRecord, Option[EntityRecord])]), Seq]

    private object EntityRecordRawSqlQuery extends RawSqlQuery {
      val driver: JdbcDriver = EntityComponent.this.driver
      implicit val getEntityRecord = GetResult { r => EntityRecord(r.<<, r.<<, r.<<, r.<<, r.<<, None) }

      def action(workspaceId: UUID, entities: Traversable[AttributeEntityReference]) = {
        val baseSelect = sql"select id, name, entity_type, workspace_id, record_version from ENTITY where workspace_id = $workspaceId and (entity_type, name) in ("
        val entityTypeNameTuples = reduceSqlActionsWithDelim(entities.map { case entity => sql"(${entity.entityType}, ${entity.entityName})" }.toSeq)
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
        val entityRec = EntityRecord(r.<<, r.<<, r.<<, r.<<, r.<<, None)

        val attributeIdOption: Option[Long] = r.<<
        val attributeRecOption = attributeIdOption.map(id => EntityAttributeRecord(id, entityRec.id, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

        val refEntityRecOption = for {
          attributeRec <- attributeRecOption
          refId <- attributeRec.valueEntityRef
        } yield {
            EntityRecord(r.<<, r.<<, r.<<, r.<<, r.<<, None)
          }

        EntityAndAttributesResult(entityRec, attributeRecOption, refEntityRecOption)
      }

      // the where clause for this query is filled in specific to the use case
      val baseEntityAndAttributeSql =
        s"""select e.id, e.name, e.entity_type, e.workspace_id, e.record_version,
          a.id, a.namespace, a.name, a.value_string, a.value_number, a.value_boolean, a.value_entity_ref, a.list_index, a.list_length,
          e_ref.id, e_ref.name, e_ref.entity_type, e_ref.workspace_id, e_ref.record_version
          from ENTITY e
          left outer join ENTITY_ATTRIBUTE a on a.owner_id = e.id
          left outer join ENTITY e_ref on a.value_entity_ref = e_ref.id"""

      def actionForTypeName(workspaceContext: SlickWorkspaceContext, entityType: String, entityName: String) = {
        sql"""#$baseEntityAndAttributeSql where e.name = ${entityName} and e.entity_type = ${entityType} and e.workspace_id = ${workspaceContext.workspaceId}""".as[EntityAndAttributesResult]
      }

      def actionForType(workspaceContext: SlickWorkspaceContext, entityType: String) = {
        sql"""#$baseEntityAndAttributeSql where e.entity_type = ${entityType} and e.workspace_id = ${workspaceContext.workspaceId}""".as[EntityAndAttributesResult]
      }

      def actionForRefs(workspaceContext: SlickWorkspaceContext, entityRefs: Traversable[AttributeEntityReference]) = {
        val baseSelect = sql"""#$baseEntityAndAttributeSql where e.workspace_id = ${workspaceContext.workspaceId} and (e.entity_type, e.name) in ("""
        val entityTypeNameTuples = reduceSqlActionsWithDelim(entityRefs.map { ref => sql"(${ref.entityType}, ${ref.entityName})" }.toSeq)
        concatSqlActions(baseSelect, entityTypeNameTuples, sql")").as[EntityAndAttributesResult]
      }

      def actionForIds(entityIds: Traversable[Long]) = {
        val baseSelect = sql"""#$baseEntityAndAttributeSql where e.id  in ("""
        val entityIdSql = reduceSqlActionsWithDelim(entityIds.map { id => sql"$id" }.toSeq)
        concatSqlActions(baseSelect, entityIdSql, sql")").as[EntityAndAttributesResult]
      }

      def actionForWorkspace(workspaceContext: SlickWorkspaceContext) = {
        sql"""#$baseEntityAndAttributeSql where e.workspace_id = ${workspaceContext.workspaceId}""".as[EntityAndAttributesResult]
      }

      def actionForId(id: Long) = {
        sql"""#$baseEntityAndAttributeSql where e.id = ${id}""".as[EntityAndAttributesResult]
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
            """, sort_a.list_length as sort_list_length, sort_a.value_string as sort_field_string, sort_a.value_number as sort_field_number, sort_a.value_boolean as sort_field_boolean, sort_e_ref.name as sort_field_ref""",
            // join to attribute and entity (for references) table, grab only the named sort attribute and only the first element of a list
            sql"""left outer join ENTITY_ATTRIBUTE sort_a on sort_a.owner_id = e.id and sort_a.name = $sortFieldName and ifnull(sort_a.list_index, 0) = 0 left outer join ENTITY sort_e_ref on sort_a.value_entity_ref = sort_e_ref.id """)
        }

        concatSqlActions(sql"""select e.id, e.name, e.all_attribute_values #$sortColumns from ENTITY e """, sortJoin, sql""" where e.entity_type = $entityType and e.workspace_id = $workspaceId """)
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
      entityAttrRec <- entityAttributeQuery if entityAttrRec.ownerId === entityId
    } yield entityAttrRec

    def findEntityByName(workspaceId: UUID, entityType: String, entityName: String): EntityQuery = {
      filter(entRec => entRec.name === entityName && entRec.entityType === entityType && entRec.workspaceId === workspaceId)
    }

    def findEntityByType(workspaceId: UUID, entityType: String): EntityQuery = {
      filter(entRec => entRec.entityType === entityType && entRec.workspaceId === workspaceId)
    }

    def findEntityByWorkspace(workspaceId: UUID): EntityQuery = {
      filter(_.workspaceId === workspaceId)
    }

    def findEntityById(id: Long): EntityQuery = {
      filter(_.id === id)
    }

    def findEntityByIdAndVersion(id: Long, version: Long): EntityQuery = {
      filter(rec => rec.id === id && rec.version === version)
    }

    def lookupEntitiesByNames(workspaceId: UUID, entities: Traversable[AttributeEntityReference]): ReadAction[Seq[EntityRecord]] = {
      if (entities.isEmpty) {
        DBIO.successful(Seq.empty)
      } else {
        // slick can't do a query with '(entityType, entityName) in ((?, ?), (?, ?), ...)' so we need raw sql
        EntityRecordRawSqlQuery.action(workspaceId, entities)
      }
    }

    /** gets the given entity */
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
      workspaceQuery.updateLastModified(workspaceContext.workspaceId) andThen
        save(workspaceContext, Seq(entity)).map(_.head)
    }

    def save(workspaceContext: SlickWorkspaceContext, entities: Traversable[Entity]): ReadWriteAction[Traversable[Entity]] = {
      entities.foreach(validateEntity)

      for {
        preExistingEntityRecs <- lookupEntitiesByNames(workspaceContext.workspaceId, entities.map(_.toReference)).map(updateEntityRecords(_, entities))
        savingEntityRecs <- insertNewEntities(workspaceContext, entities, preExistingEntityRecs).map(_ ++ preExistingEntityRecs)
        referencedAndSavingEntityRecs <- lookupNotYetLoadedReferences(workspaceContext, entities, savingEntityRecs).map(_ ++ savingEntityRecs)
        _ <- upsertAttributes(entities, (savingEntityRecs ++ preExistingEntityRecs), referencedAndSavingEntityRecs)
        _ <- DBIO.seq(preExistingEntityRecs map optimisticLockUpdate: _ *)
      } yield entities
    }

    def updateEntityRecords(entityRecs: Seq[EntityRecord], entities: Traversable[Entity]): Seq[EntityRecord] = {
      val entitiesByRef = entities.map(e => e.toReference -> e).toMap
      entityRecs.map { rec =>
        rec.copy(allAttributeValues = createAllAttributesString(entitiesByRef(AttributeEntityReference(rec.entityType, rec.name))))
      }
    }

    private def optimisticLockUpdate(originalRec: EntityRecord): ReadWriteAction[Int] = {
      findEntityByIdAndVersion(originalRec.id, originalRec.recordVersion) update originalRec.copy(recordVersion = originalRec.recordVersion + 1) map {
        case 0 => throw new RawlsConcurrentModificationException(s"could not update $originalRec because its record version has changed")
        case success => success
      }
    }

    private def upsertAttributes(entities: Traversable[Entity], entityRecs: Traversable[EntityRecord], referencedAndSavingEntityRecs: Traversable[EntityRecord]) = {
      val entityIds = entityRecs.map(_.id).toSeq

      def insertScratchAttributes(transactionId: String): ReadWriteAction[Unit] = {
        val entityIdsByName = referencedAndSavingEntityRecs.map(r => AttributeEntityReference(r.entityType, r.name) -> r.id).toMap
        val attributeRecsToEntityId = for {
          entity <- entities
          (attributeName, attribute) <- entity.attributes
          attributeRec <- entityAttributeQuery.marshalAttribute(entityIdsByName(entity.toReference), attributeName, attribute, entityIdsByName)
        } yield attributeRec

        entityAttributeScratchQuery.batchInsertAttributes(attributeRecsToEntityId.toSeq, transactionId)
      }

      entityAttributeQuery.AlterAttributesUsingScratchTableQueries.upsertAction(entityIds, insertScratchAttributes)
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
      } yield ref).toSet -- alreadyLoadedEntityRecs.map(r => AttributeEntityReference(r.entityType, r.name))

      lookupEntitiesByNames(workspaceContext.workspaceId, notYetLoadedEntityRecs) map { foundEntities =>
        if (foundEntities.size != notYetLoadedEntityRecs.size) {
          val notFoundRefs = notYetLoadedEntityRecs -- foundEntities.map(r => AttributeEntityReference(r.entityType, r.name))
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

    /** deletes an entity */
    def delete(workspaceContext: SlickWorkspaceContext, entityType: String, entityName: String): ReadWriteAction[Boolean] = {
      workspaceQuery.updateLastModified(workspaceContext.workspaceId) andThen
        uniqueResult[EntityRecord](findEntityByName(workspaceContext.workspaceId, entityType, entityName)) flatMap {
          case None => DBIO.successful(false)
          case Some(entityRec) =>
            val deleteActions = deleteEntityAttributes(Seq(entityRec))
            val deleteEntity = findEntityByName(workspaceContext.workspaceId, entityType, entityName).delete
            deleteActions andThen deleteEntity.map(_ > 0)
        }
    }

    object DeleteEntityAttributesQuery extends RawSqlQuery {
      val driver: JdbcDriver = EntityComponent.this.driver

      def deleteAction(workspaceId: UUID) =
        sqlu"""delete ea from ENTITY_ATTRIBUTE ea
               inner join ENTITY e
               on ea.owner_id = e.id
               where e.workspace_id=${workspaceId}
          """
    }


    /** list all entities of the given type in the workspace */
    def list(workspaceContext: SlickWorkspaceContext, entityType: String): ReadAction[TraversableOnce[Entity]] = {
      unmarshalEntities(EntityAndAttributesRawSqlQuery.actionForType(workspaceContext, entityType))
    }

    def list(workspaceContext: SlickWorkspaceContext, entityRefs: Traversable[AttributeEntityReference]): ReadAction[TraversableOnce[Entity]] = {
      unmarshalEntities(EntityAndAttributesRawSqlQuery.actionForRefs(workspaceContext, entityRefs))
    }

    def listByIds(entityIds: Traversable[Long]): ReadAction[Seq[(Long, Entity)]] = {
      unmarshalEntitiesWithIds(EntityAndAttributesRawSqlQuery.actionForIds(entityIds))
    }

    def rename(workspaceContext: SlickWorkspaceContext, entityType: String, oldName: String, newName: String): ReadWriteAction[Int] = {
      workspaceQuery.updateLastModified(workspaceContext.workspaceId) andThen
        findEntityByName(workspaceContext.workspaceId, entityType, oldName).map(_.name).update(newName)
    }

    def getEntityTypes(workspaceContext: SlickWorkspaceContext): ReadAction[TraversableOnce[String]] = {
      filter(_.workspaceId === workspaceContext.workspaceId).map(_.entityType).distinct.result
    }

    def getEntityTypesWithCounts(workspaceContext: SlickWorkspaceContext): ReadAction[Map[String, Int]] = {
      filter(_.workspaceId === workspaceContext.workspaceId).groupBy(e => e.entityType).map { case (entityType, entities) =>
        (entityType, entities.length)
      }.result map { result =>
        result.toMap
      }
    }

    def getAttrNamesAndEntityTypes(workspaceContext: SlickWorkspaceContext): ReadAction[Map[String, Seq[String]]] = {
      val typesAndAttrNames = for {
        entityRec <- filter(_.workspaceId === workspaceContext.workspaceId)
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
            (entityType, EntityTypeMetadata( typesAndCounts.getOrElse(entityType, 0), typesAndAttrs.getOrElse(entityType, Seq()) ))
          } toMap
        }
      }
    }

    def listEntitiesAllTypes(workspaceContext: SlickWorkspaceContext): ReadAction[TraversableOnce[Entity]] = {
      unmarshalEntities(EntityAndAttributesRawSqlQuery.actionForWorkspace(workspaceContext))
    }

    def cloneAllEntities(sourceWorkspaceContext: SlickWorkspaceContext, destWorkspaceContext: SlickWorkspaceContext): ReadWriteAction[Unit] = {
      val allEntitiesAction = listEntitiesAllTypes(sourceWorkspaceContext)

      allEntitiesAction.flatMap(cloneEntities(destWorkspaceContext, _))
    }

    def batchInsertEntities(workspaceContext: SlickWorkspaceContext, entities: Seq[EntityRecord]): ReadWriteAction[Seq[EntityRecord]] = {
      if(!entities.isEmpty) {
        workspaceQuery.updateLastModified(workspaceContext.workspaceId) andThen
          insertInBatches(entityQuery, entities) andThen selectEntityIds(workspaceContext, entities)
      }
      else {
        DBIO.successful(Seq.empty[EntityRecord])
      }
    }

    def selectEntityIds(workspaceContext: SlickWorkspaceContext, entities: Seq[EntityRecord]): ReadAction[Seq[EntityRecord]] = {
      val entitiesGrouped = entities.grouped(batchSize).toSeq

      DBIO.sequence(entitiesGrouped map { batch =>
        EntityRecordRawSqlQuery.action(workspaceContext.workspaceId, batch.map(r => AttributeEntityReference(r.entityType, r.name)))
      }).map(_.flatten)
    }

    def cloneEntities(destWorkspaceContext: SlickWorkspaceContext, entities: TraversableOnce[Entity]): ReadWriteAction[Unit] = {
      batchInsertEntities(destWorkspaceContext, entities.toSeq.map(marshalNewEntity(_, destWorkspaceContext.workspaceId))) flatMap { ids =>
        val entityIdByEntity = ids.map(record => record.id -> entities.filter(p => p.entityType == record.entityType && p.name == record.name).toSeq.head)
        val entityIdsByRef = entityIdByEntity.map { case (entityId, entity) => entity.toReference -> entityId }.toMap

        val attributeRecords = entityIdByEntity flatMap { case (entityId, entity) =>
          entity.attributes.flatMap { case (attributeName, attr) =>
            entityAttributeQuery.marshalAttribute(entityId, attributeName, attr, entityIdsByRef)
          }
        }

        entityAttributeQuery.batchInsertAttributes(attributeRecords)
      }
    }

    /**
     * Starting with entities specified by entityIds, recursively walk down references accumulating all the ids
     * @param entityIds the ids to start with
     * @param accumulatedIds the ids accumulated from the prior call. If you wish entityIds to be in the overall
     *                       results, start with entityIds == accumulatedIds, otherwise start with Seq.empty but note
     *                       that if there is a cycle some of entityIds may be in the result anyway
     * @return the ids of all the entities referred to by entityIds
     */
    private def recursiveGetEntityReferenceIds(entityIds: Set[Long], accumulatedIds: Set[Long]): ReadAction[Set[Long]] = {
      // need to batch because some RDBMSes have a limit on the length of an in clause
      val batchedEntityIds = createBatches(entityIds)

      val batchQueries = batchedEntityIds.map {
        idBatch => filter(_.id inSetBind(idBatch)) join
          entityAttributeQuery on (_.id === _.ownerId) filter(_._2.valueEntityRef.isDefined) map (_._2.valueEntityRef)
      }

      val referencesResults = DBIO.sequence(batchQueries.map(_.result))

      referencesResults.map(_.reduce(_ ++ _)).flatMap { refIdOptions =>
        val refIds = refIdOptions.collect { case Some(id) => id }.toSet
        val untraversedIds = refIds -- accumulatedIds
        if (untraversedIds.isEmpty) {
          DBIO.successful(accumulatedIds)
        } else {
          recursiveGetEntityReferenceIds(untraversedIds, accumulatedIds ++ refIds)
        }
      }
    }

    /**
     * used in copyEntities load all the entities to copy
     */
    def getEntitySubtrees(workspaceContext: SlickWorkspaceContext, entityType: String, entityNames: Seq[String]): ReadAction[TraversableOnce[Entity]] = {
      val startingEntityIdsAction = filter(rec => rec.workspaceId === workspaceContext.workspaceId && rec.entityType === entityType && rec.name.inSetBind(entityNames)).map(_.id)
      val entitiesQuery = startingEntityIdsAction.result.flatMap { startingEntityIds =>
        val idSet = startingEntityIds.toSet
        recursiveGetEntityReferenceIds(idSet, idSet)
      } flatMap { ids =>
        DBIO.sequence(ids.map { id =>
          unmarshalEntities(EntityAndAttributesRawSqlQuery.actionForId(id))
        }.toSeq)
      }
      entitiesQuery.map(_.flatten)
    }

    def copyEntities(sourceWorkspaceContext: SlickWorkspaceContext, destWorkspaceContext: SlickWorkspaceContext, entityType: String, entityNames: Seq[String]): ReadWriteAction[TraversableOnce[Entity]] = {
      getEntitySubtrees(sourceWorkspaceContext, entityType, entityNames).flatMap { entities =>
        getCopyConflicts(destWorkspaceContext, entities).flatMap { conflicts =>
          if (conflicts.isEmpty) {
            cloneEntities(destWorkspaceContext, entities).map(_ => Seq.empty[Entity])
          } else {
            DBIO.successful(conflicts)
          }
        }
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

    def marshalNewEntity(entity: Entity, workspaceId: UUID): EntityRecord = {
      EntityRecord(0, entity.name, entity.entityType, workspaceId, 0, createAllAttributesString(entity))
    }

    def unmarshalEntity(entityRecord: EntityRecord, attributes: AttributeMap) = {
      Entity(entityRecord.name, entityRecord.entityType, attributes)
    }

    def deleteEntityAttributes(entityRecords: Seq[EntityRecord]) = {
      entityAttributeQuery.filter(_.ownerId.inSetBind(entityRecords.map(_.id))).delete
    }
  }

  def createAllAttributesString(entity: Entity): Option[String] = {
    Option(s"${entity.name} ${entity.attributes.values.filterNot(_.isInstanceOf[AttributeList[_]]).map(AttributeStringifier(_)).mkString(" ")}".toLowerCase)
  }

  def validateEntity(entity: Entity): Unit = {
    validateUserDefinedString(entity.entityType) // do we need to check this here if we're already validating all edges?
    validateUserDefinedString(entity.name)
    entity.attributes.keys.foreach { attrName =>
      validateUserDefinedString(attrName.name)
      validateAttributeName(attrName)
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
