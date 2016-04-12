package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.util.UUID
import javax.xml.bind.DatatypeConverter

import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, RawlsException}
import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import slick.dbio.Effect.Read
import slick.jdbc.GetResult
import org.broadinstitute.dsde.rawls.model._
import slick.jdbc.GetResult
import spray.http.StatusCodes

/**
 * Created by dvoet on 2/4/16.
 */
case class EntityRecord(id: UUID, name: String, entityType: String, workspaceId: UUID)
case class EntityAttributeRecord(entityId: UUID, attributeId: UUID)

trait EntityComponent {
  this: DriverComponent with WorkspaceComponent with AttributeComponent =>

  import driver.api._

  class EntityTable(tag: Tag) extends Table[EntityRecord](tag, "ENTITY") {
    def id = column[UUID]("id", O.PrimaryKey)
    def name = column[String]("name", O.Length(254))
    def entityType = column[String]("entity_type", O.Length(254))
    def workspaceId = column[UUID]("workspace_id")
    def workspace = foreignKey("FK_ENTITY_WORKSPACE", workspaceId, workspaceQuery)(_.id)
    def uniqueTypeName = index("idx_entity_type_name", (workspaceId, entityType, name), unique = true)
    def * = (id, name, entityType, workspaceId) <> (EntityRecord.tupled, EntityRecord.unapply)
  }

  class EntityAttributeTable(tag: Tag) extends Table[EntityAttributeRecord](tag, "ENTITY_ATTRIBUTE") {
    def entityId = column[UUID]("entity_id")
    def attributeId = column[UUID]("attribute_id", O.PrimaryKey)

    def entity = foreignKey("FK_ENT_ATTR_ENTITY", entityId, entityQuery)(_.id)
    def attribute = foreignKey("FK_ENT_ATTR_ATTRIBUTE", attributeId, attributeQuery)(_.id, onDelete = ForeignKeyAction.Cascade)

    def * = (entityId, attributeId) <> (EntityAttributeRecord.tupled, EntityAttributeRecord.unapply)
  }

  protected val entityAttributeQuery = TableQuery[EntityAttributeTable]
  
  object entityQuery extends TableQuery(new EntityTable(_)) {
    type EntityQuery = Query[EntityTable, EntityRecord, Seq]
    type EntityQueryWithAttributesAndRefs =  Query[(EntityTable, Rep[Option[(AttributeTable, Rep[Option[EntityTable]])]]), (EntityRecord, Option[(AttributeRecord, Option[EntityRecord])]), Seq]

    implicit val getEntityRecord = GetResult { r => EntityRecord(r.<<, r.<<, r.<<, r.<<) }

    // result structure from entity and attribute list raw sql
    case class EntityListResult(entityRecord: EntityRecord, attributeRecord: Option[AttributeRecord], refEntityRecord: Option[EntityRecord])

    // tells slick how to convert a result row from a raw sql query to an instance of EntityListResult
    implicit val getEntityListResult = GetResult { r =>
      // note that the number and order of all the r.<< match precisely with the select clause of baseEntityAndAttributeSql
      val entityRec = EntityRecord(r.<<, r.<<, r.<<, r.<<)

      val attributeIdOption: Option[UUID] = r.<<
      val attributeRecOption = attributeIdOption.map(id => AttributeRecord(id, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

      val refEntityRecOption = for {
        attributeRec <- attributeRecOption
        refId <- attributeRec.valueEntityRef
      } yield { EntityRecord(r.<<, r.<<, r.<<, r.<<) }

      EntityListResult(entityRec, attributeRecOption, refEntityRecOption)
    }

    import driver.quoteIdentifier
    // the where clause for this query is filled in specific to the use case
    val baseEntityAndAttributeSql =
      s"""select e.${quoteIdentifier("id")}, e.${quoteIdentifier("name")}, e.${quoteIdentifier("entity_type")}, e.${quoteIdentifier("workspace_id")}, a.${quoteIdentifier("id")}, a.${quoteIdentifier("name")}, a.${quoteIdentifier("value_string")}, a.${quoteIdentifier("value_number")}, a.${quoteIdentifier("value_boolean")}, a.${quoteIdentifier("value_entity_ref")}, a.${quoteIdentifier("list_index")}, e_ref.${quoteIdentifier("id")}, e_ref.${quoteIdentifier("name")}, e_ref.${quoteIdentifier("entity_type")}, e_ref.${quoteIdentifier("workspace_id")}
          from ENTITY e
          left outer join ENTITY_ATTRIBUTE ea on e.${quoteIdentifier("id")} = ea.${quoteIdentifier("entity_id")}
          left outer join ATTRIBUTE a on ea.${quoteIdentifier("attribute_id")} = a.${quoteIdentifier("id")}
          left outer join ENTITY e_ref on a.${quoteIdentifier("value_entity_ref")} = e_ref.${quoteIdentifier("id")}"""

    def entityAttributes(entityId: UUID) = for {
      entityAttrRec <- entityAttributeQuery if entityAttrRec.entityId === entityId
      attributeRec <- attributeQuery if entityAttrRec.attributeId === attributeRec.id
    } yield attributeRec

    def findEntityByName(workspaceId: UUID, entityType: String, entityName: String): EntityQuery = {
      filter(entRec => entRec.name === entityName && entRec.entityType === entityType && entRec.workspaceId === workspaceId)
    }

    def findEntityByType(workspaceId: UUID, entityType: String): EntityQuery = {
      filter(entRec => entRec.entityType === entityType && entRec.workspaceId === workspaceId)
    }

    def findEntityByWorkspace(workspaceId: UUID): EntityQuery = {
      filter(_.workspaceId === workspaceId)
    }

    def findEntityById(id: UUID): EntityQuery = {
      filter(_.id === id)
    }

    def lookupEntitiesByNames(workspaceId: UUID, entities: Traversable[AttributeEntityReference]): ReadAction[Seq[EntityRecord]] = {
      if (entities.isEmpty) {
        DBIO.successful(Seq.empty)
      } else {
        // slick can't do a query with '(entityType, entityName) in ((?, ?), (?, ?), ...)' so we need raw sql
        val baseSelect = sql"select id, name, entity_type, workspace_id from ENTITY where workspace_id = $workspaceId and (entity_type, name) in ("
        val entityTypeNameTuples = entities.map { case entity => sql"(${entity.entityType}, ${entity.entityName})" }.reduce((a, b) => concatSqlActionsWithDelim(a, b, sql","))
        concatSqlActions(concatSqlActions(baseSelect, entityTypeNameTuples), sql")").as[EntityRecord]
      }
    }

    /** gets the given entity */
    def get(workspaceContext: SlickWorkspaceContext, entityType: String, entityName: String): ReadAction[Option[Entity]] = {
      val sql = sql"""#$baseEntityAndAttributeSql where e.#${quoteIdentifier("name")} = ${entityName} and e.#${quoteIdentifier("entity_type")} = ${entityType} and e.#${quoteIdentifier("workspace_id")} = ${workspaceContext.workspaceId}""".as[EntityListResult]
      unmarshalEntities(sql).map(_.headOption)
    }

    /**
     * converts a query resulting in a number of records representing many entities with many attributes, some of them references
     */
    def unmarshalEntities(entityAndAttributesQuery: EntityQueryWithAttributesAndRefs): ReadAction[Iterable[Entity]] = {
      entityAndAttributesQuery.result map { entityAttributeRecords =>
        val entityRecords = entityAttributeRecords.map(_._1).toSet
        val attributesByEntityId = attributeQuery.unmarshalAttributes(entityAttributeRecords.collect {
          case (entityRec, Some((attributeRec, referenceOption))) => ((entityRec.id, attributeRec), referenceOption)
        })

        entityRecords.map { entityRec =>
          unmarshalEntity(entityRec, attributesByEntityId.getOrElse(entityRec.id, Map.empty))
        }
      }
    }

    def unmarshalEntities(entityAttributeAction: ReadAction[Seq[EntityListResult]]): ReadAction[Iterable[Entity]] = {
      unmarshalEntitiesWithIds(entityAttributeAction).map(_.map { case (id, entity) => entity })
    }

    def unmarshalEntitiesWithIds(entityAttributeAction: ReadAction[Seq[EntityListResult]]): ReadAction[Map[UUID, Entity]] = {
      entityAttributeAction.map { entityAttributeRecords =>
        val allEntityRecords = entityAttributeRecords.map(_.entityRecord).toSet

        // note that not all entities have attributes, thus the collect below
        val entitiesWithAttributes = entityAttributeRecords.collect {
          case EntityListResult(entityRec, Some(attributeRec), refEntityRecOption) => ((entityRec.id, attributeRec), refEntityRecOption)
        }

        val attributesByEntityId = attributeQuery.unmarshalAttributes[UUID](entitiesWithAttributes)

        allEntityRecords.map { entityRec =>
          entityRec.id -> unmarshalEntity(entityRec, attributesByEntityId.getOrElse(entityRec.id, Map.empty))
        }.toMap
      }
    }

    /** creates or replaces an entity */
    def save(workspaceContext: SlickWorkspaceContext, entity: Entity): ReadWriteAction[Entity] = {
      save(workspaceContext, Seq(entity)).map(_.head)
    }

    def save(workspaceContext: SlickWorkspaceContext, entities: Traversable[Entity]): ReadWriteAction[Traversable[Entity]] = {
      entities.foreach(validateEntity)

      for {
        preExistingEntityRecs <- lookupEntitiesByNames(workspaceContext.workspaceId, entities.map(_.toReference))
        _ <- deleteEntityAttributes(preExistingEntityRecs)
        savingEntityRecs <- insertNewEntities(workspaceContext, entities, preExistingEntityRecs).map(_ ++ preExistingEntityRecs)
        referencedAndSavingEntityRecs <- lookupNotYetLoadedReferences(workspaceContext, entities, savingEntityRecs).map(_ ++ savingEntityRecs)
        _ <- insertAttributes(entities, referencedAndSavingEntityRecs)
      } yield entities
    }

    private def insertAttributes(entities: Traversable[Entity], entityRecs: Traversable[EntityRecord]) = {
      val entityIdsByName = entityRecs.map(r => AttributeEntityReference(r.entityType, r.name) -> r.id).toMap
      val attributeRecsToEntityId = (for {
        entity <- entities
        (attributeName, attribute) <- entity.attributes
        attributeRec <- attributeQuery.marshalAttribute(attributeName, attribute, entityIdsByName)
      } yield attributeRec -> entityIdsByName(entity.toReference)).toMap

      attributeQuery.insertAttributesSql(attributeRecsToEntityId.keys.toSeq) andThen
        insertEntityAttributesSql(attributeRecsToEntityId.map { case (attr, entityId) => attr.id -> entityId })
    }

    private def lookupNotYetLoadedReferences(workspaceContext: SlickWorkspaceContext, entities: Traversable[Entity], alreadyLoadedEntityRecs: Traversable[EntityRecord]) = {
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

    private def insertNewEntities(workspaceContext: SlickWorkspaceContext, entities: Traversable[Entity], preExistingEntityRecs: Seq[EntityRecord]): WriteAction[Traversable[EntityRecord]] = {
      val existingEntityTypeNames = preExistingEntityRecs.map(rec => (rec.entityType, rec.name))
      val newEntities = entities.filterNot(e => existingEntityTypeNames.exists(_ ==(e.entityType, e.name)))

      val newEntityRecs = newEntities.map(e => marshalEntity(UUID.randomUUID(), e, workspaceContext.workspaceId))
      insertEntitiesSql(newEntityRecs.toSeq).map(_ => newEntityRecs)
    }

    /** deletes an entity */
    def delete(workspaceContext: SlickWorkspaceContext, entityType: String, entityName: String): ReadWriteAction[Boolean] = {
      uniqueResult[EntityRecord](findEntityByName(workspaceContext.workspaceId, entityType, entityName)) flatMap {
        case None => DBIO.successful(false)
        case Some(entityRec) =>
          val deleteActions = deleteEntityAttributes(Seq(entityRec))
          val deleteEntity = findEntityByName(workspaceContext.workspaceId, entityType, entityName).delete
          deleteActions andThen deleteEntity.map(_ > 0)
      }
    }

    /** list all entities of the given type in the workspace */
    def list(workspaceContext: SlickWorkspaceContext, entityType: String): ReadAction[TraversableOnce[Entity]] = {
      val sql = sql"""#$baseEntityAndAttributeSql where e.#${quoteIdentifier("entity_type")} = ${entityType} and e.#${quoteIdentifier("workspace_id")} = ${workspaceContext.workspaceId}""".as[EntityListResult]
      unmarshalEntities(sql)
    }

    def list(workspaceContext: SlickWorkspaceContext, entityRefs: Traversable[AttributeEntityReference]): ReadAction[TraversableOnce[Entity]] = {
      val baseSelect = sql"""#$baseEntityAndAttributeSql where e.#${quoteIdentifier("workspace_id")} = ${workspaceContext.workspaceId} and (e.#${quoteIdentifier("entity_type")}, e.#${quoteIdentifier("name")}) in ("""
      val entityTypeNameTuples = entityRefs.map { ref => sql"(${ref.entityType}, ${ref.entityName})" }.reduce((a, b) => concatSqlActionsWithDelim(a, b, sql","))
      unmarshalEntities(concatSqlActions(concatSqlActions(baseSelect, entityTypeNameTuples), sql")").as[EntityListResult])
    }

    def listByIds(entityIds: Traversable[UUID]): ReadAction[Map[UUID, Entity]] = {
      val baseSelect = sql"""#$baseEntityAndAttributeSql where e.#${quoteIdentifier("id")}  in ("""
      val entityIdSql = entityIds.map { id => sql"$id" }.reduce((a, b) => concatSqlActionsWithDelim(a, b, sql","))
      unmarshalEntitiesWithIds(concatSqlActions(concatSqlActions(baseSelect, entityIdSql), sql")").as[EntityListResult])
    }

    /**
     * Extends given query to query for attributes (if they exist) and entity references (if they exist).
     * query joinLeft entityAttributeQuery join attributeQuery joinLeft entityQuery
     * @param query
     * @return
     */
    def joinOnAttributesAndRefs(query: EntityQuery): EntityQueryWithAttributesAndRefs = {
      query joinLeft {
        entityAttributeQuery join attributeQuery on (_.attributeId === _.id) joinLeft
          entityQuery on (_._2.valueEntityRef === _.id)
      } on (_.id === _._1._1.entityId) map { result =>
        (result._1, result._2.map { case (a, b) => (a._2, b) })
      }
    }

    def rename(workspaceContext: SlickWorkspaceContext, entityType: String, oldName: String, newName: String): ReadWriteAction[Int] = {
      findEntityByName(workspaceContext.workspaceId, entityType, oldName).map(_.name).update(newName)
    }

    def getEntityTypes(workspaceContext: SlickWorkspaceContext): ReadAction[TraversableOnce[String]] = {
      filter(_.workspaceId === workspaceContext.workspaceId).map(_.entityType).distinct.result
    }

    def getEntityTypesWithCounts(workspaceContext: SlickWorkspaceContext): ReadAction[Map[String, Int]] = {
      filter(_.workspaceId === workspaceContext.workspaceId).groupBy(e => e.entityType).map { case (entityType, entities) =>
        (entityType, entities.countDistinct)
      }.result map { result =>
        result.toMap
      }
    }

    def listEntitiesAllTypes(workspaceContext: SlickWorkspaceContext): ReadAction[TraversableOnce[Entity]] = {
      val sql = sql"""#$baseEntityAndAttributeSql where e.#${quoteIdentifier("workspace_id")} = ${workspaceContext.workspaceId}""".as[EntityListResult]
      unmarshalEntities(sql)
    }

    def cloneAllEntities(sourceWorkspaceContext: SlickWorkspaceContext, destWorkspaceContext: SlickWorkspaceContext): ReadWriteAction[Unit] = {
      val allEntitiesAction = listEntitiesAllTypes(sourceWorkspaceContext)

      allEntitiesAction.flatMap(cloneEntities(destWorkspaceContext, _))
    }

    private def insertEntitiesSql(entities: Seq[EntityRecord]): driver.api.DBIOAction[Unit, driver.api.NoStream, driver.api.Effect] = {
      val insertBatches = entities.map { entity =>
        sql"(${entity.id}, ${entity.name}, ${entity.entityType}, ${entity.workspaceId})"
      }.grouped(batchSize).toSeq

      DBIO.seq(insertBatches.map { insertBatch =>
        val prefix = sql"insert into ENTITY (id, name, entity_type, workspace_id) values "
        val suffix = insertBatch.reduce { (a, b) =>
          concatSqlActionsWithDelim(a, b, sql", ")
        }
        concatSqlActions(prefix, suffix).as[Int]
      }:_*)
    }

    private def insertEntityAttributesSql(attributeIdToEntityId: Map[UUID, UUID]) = {
      val insertBatches = attributeIdToEntityId.map { case (attrId, entityId) =>
          sql"($entityId, $attrId)"
      }.grouped(batchSize).toSeq

      DBIO.seq(insertBatches.map { insertBatch =>
        val prefix = sql"insert into ENTITY_ATTRIBUTE (entity_id, attribute_id) values "
        val suffix = insertBatch.reduce { (a, b) =>
          concatSqlActionsWithDelim(a, b, sql", ")
        }
        concatSqlActions(prefix, suffix).as[Int]
      }:_*)
    }

    def cloneEntities(destWorkspaceContext: SlickWorkspaceContext, entities: TraversableOnce[Entity]): ReadWriteAction[Unit] = {

      val entityIdsByName = entities.map { entity =>
        entity.toReference -> UUID.randomUUID()
      }.toMap

      val entityRecordsWithAttributes = entities.map { entity =>
        marshalEntity(entityIdsByName(entity.toReference), entity, destWorkspaceContext.workspaceId) -> entity.attributes
      }.toSeq

      val attributeRecordsWithEntity = entityRecordsWithAttributes.flatMap { case (entity, attrs) =>
        attrs.flatMap { case (name, attr) =>
          attributeQuery.marshalAttribute(name, attr, entityIdsByName).map(_ -> entity)
        }
      }

      val attributesWithEntityIds = attributeRecordsWithEntity.map { case (attrRec, entityRec) =>
        (attrRec.id, entityRec.id)
      }.toMap

      insertEntitiesSql(entityRecordsWithAttributes.map(_._1)) andThen
        attributeQuery.insertAttributesSql(attributeRecordsWithEntity.map(_._1)) andThen
        insertEntityAttributesSql(attributesWithEntityIds)
    }

    /**
     * Starting with entities specified by entityIds, recursively walk down references accumulating all the ids
     * @param entityIds the ids to start with
     * @param accumulatedIds the ids accumulated from the prior call. If you wish entityIds to be in the overall
     *                       results, start with entityIds == accumulatedIds, otherwise start with Seq.empty but note
     *                       that if there is a cycle some of entityIds may be in the result anyway
     * @return the ids of all the entities referred to by entityIds
     */
    private def recursiveGetEntityReferenceIds(entityIds: Set[UUID], accumulatedIds: Set[UUID]): ReadAction[Set[UUID]] = {
      // need to batch because some RDBMSes have a limit on the length of an in clause
      val batchedEntityIds = createBatches(entityIds)

      val batchQueries = batchedEntityIds.map {
        idBatch => filter(_.id inSetBind(idBatch)) join
          entityAttributeQuery on (_.id === _.entityId) join
          attributeQuery on (_._2.attributeId === _.id) filter(_._2.valueEntityRef.isDefined) map (_._2.valueEntityRef)
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
          val sql = sql"""#$baseEntityAndAttributeSql where e.#${quoteIdentifier("id")} = ${id}""".as[EntityListResult]
          unmarshalEntities(sql)
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

    def marshalEntity(entityId: UUID, entity: Entity, workspaceId: UUID): EntityRecord = {
      EntityRecord(entityId, entity.name, entity.entityType, workspaceId)
    }

    def unmarshalEntity(entityRecord: EntityRecord, attributes: Map[String, Attribute]) = {
      Entity(entityRecord.name, entityRecord.entityType, attributes)
    }

    private def insertEntityAttributes(entity: Entity, entityId: UUID, workspaceId: UUID): Seq[ReadWriteAction[Int]] = {
      val attributeInserts = entity.attributes.flatMap { case (name, attribute) =>
        attributeQuery.insertAttributeRecords(name, attribute, workspaceId)
      } map (_.flatMap { attributeId =>
        entityAttributeQuery += EntityAttributeRecord(entityId, attributeId)
      })
      attributeInserts.toSeq
    }

    def deleteEntityAttributes(entityRecords: Seq[EntityRecord]) = {
      val entityAttributes = entityAttributeQuery.filter(_.entityId.inSetBind(entityRecords.map(_.id)))
      attributeQuery.filter(_.id in entityAttributes.map(_.attributeId)).delete
    }
  }

  def validateEntity(entity: Entity): Unit = {
    validateUserDefinedString(entity.entityType) // do we need to check this here if we're already validating all edges?
    validateUserDefinedString(entity.name)
    entity.attributes.keys.foreach { value =>
      validateUserDefinedString(value)
      validateAttributeName(value)
    }
  }
}
