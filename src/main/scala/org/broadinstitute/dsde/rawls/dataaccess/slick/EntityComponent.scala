package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.util.UUID

import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import org.broadinstitute.dsde.rawls.model.{Attribute, Entity}

/**
 * Created by dvoet on 2/4/16.
 */
case class EntityRecord(id: Long, name: String, entityType: String, workspaceId: UUID)
case class EntityAttributeRecord(entityId: Long, attributeId: Long)

trait EntityComponent {
  this: DriverComponent with WorkspaceComponent with AttributeComponent =>

  import driver.api._

  class EntityTable(tag: Tag) extends Table[EntityRecord](tag, "ENTITY") {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def name = column[String]("name")
    def entityType = column[String]("entity_type")
    def workspaceId = column[UUID]("workspace_id")
    def workspace = foreignKey("FK_ENTITY_WORKSPACE", workspaceId, workspaceQuery)(_.id)
    def uniqueTypeName = index("idx_entity_type_name", (workspaceId, entityType, name), unique = true)
    def * = (id, name, entityType, workspaceId) <> (EntityRecord.tupled, EntityRecord.unapply)
  }

  class EntityAttributeTable(tag: Tag) extends Table[EntityAttributeRecord](tag, "ENTITY_ATTRIBUTE") {
    def entityId = column[Long]("entity_id")
    def attributeId = column[Long]("attribute_id", O.PrimaryKey)

    def entity = foreignKey("FK_ENT_ATTR_ENTITY", entityId, entityQuery)(_.id)
    def attribute = foreignKey("FK_ENT_ATTR_ATTRIBUTE", attributeId, attributeQuery)(_.id)

    def * = (entityId, attributeId) <> (EntityAttributeRecord.tupled, EntityAttributeRecord.unapply)
  }

  protected val entityAttributeQuery = TableQuery[EntityAttributeTable]
  
  object entityQuery extends TableQuery(new EntityTable(_)) {
    type EntityQuery = Query[EntityTable, EntityRecord, Seq]
    type EntityQueryWithAttributesAndRefs = Query[(EntityTable, Rep[Option[((EntityAttributeTable, AttributeTable), Rep[Option[EntityTable]])]]), (EntityRecord, Option[((EntityAttributeRecord, AttributeRecord), Option[EntityRecord])]), Seq]

    private def entityAttributesWithRefsJoin = {
      entityAttributeQuery join attributeQuery on (_.attributeId === _.id) joinLeft
        entityQuery on (_._2.valueEntityRef === _.id)
    }

    private def entityAttributes(entityId: Long) = for {
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

    def findEntityById(id: Long): EntityQuery = {
      filter(_.id === id)
    }

    /** gets the given entity */
    def get(workspaceContext: SlickWorkspaceContext, entityType: String, entityName: String): ReadAction[Option[Entity]] = {
      // query gets an entity record, all of its attributes (if there are any), and referred entities (if there are any)
      val entityAndAttributesQuery =
        findEntityByName(workspaceContext.workspaceId, entityType, entityName) joinLeft entityAttributesWithRefsJoin on (_.id === _._1._1.entityId)

      unmarshalEntities(entityAndAttributesQuery).map(_.headOption)
    }

    /**
     * converts a query resulting in a number of records representing many entities with many attributes, some of them references
     */
    private def unmarshalEntities(entityAndAttributesQuery: EntityQueryWithAttributesAndRefs): ReadAction[Iterable[Entity]] = {
      entityAndAttributesQuery.result map { entityAttributeRecords =>
        val attributeRecsByEntityRec = entityAttributeRecords.groupBy(_._1)
        attributeRecsByEntityRec map { case (entityRec, attributeRecs) =>
          val attributes = attributeQuery.unmarshalAttributes(attributeRecs.collect {
            case (_, Some(((_, attributeRec), entityRef: Option[EntityRecord]))) => (attributeRec, entityRef)
          })
          unmarshalEntity(entityRec, attributes)
        }
      }
    }

    /** creates or replaces an entity */
    def save(workspaceContext: SlickWorkspaceContext, entity: Entity): ReadWriteAction[Entity] = {
      validateUserDefinedString(entity.entityType) // do we need to check this here if we're already validating all edges?
      validateUserDefinedString(entity.name)
      entity.attributes.keys.foreach { value =>
        validateUserDefinedString(value)
        validateAttributeName(value)
      }

      uniqueResult[EntityRecord](findEntityByName(workspaceContext.workspaceId, entity.entityType, entity.name)) flatMap {
        case None =>
          val entityInsert = (entityQuery returning entityQuery.map(_.id)) += EntityRecord(0, entity.name, entity.entityType, workspaceContext.workspaceId)
          entityInsert flatMap { entityId =>
            DBIO.seq(insertEntityAttributes(entity, entityId, workspaceContext.workspaceId):_*)
          }
        case Some(entityRec) =>
          // note that there is nothing in the entity record itself that is actually updateable
          entityAttributes(entityRec.id).result.flatMap { attributeRecords =>
            val deleteActions = deleteEntityAttributes(attributeRecords)
            val insertActions = insertEntityAttributes(entity, entityRec.id, workspaceContext.workspaceId)
            DBIO.seq(deleteActions ++ insertActions:_*)
          }
      } map { _ => entity }
    }

    /** deletes an entity */
    def delete(workspaceContext: SlickWorkspaceContext, entityType: String, entityName: String): ReadWriteAction[Boolean] = {
      uniqueResult[EntityRecord](findEntityByName(workspaceContext.workspaceId, entityType, entityName)) flatMap {
        case None => DBIO.successful(false)
        case Some(entityRec) =>
          entityAttributes(entityRec.id).result.flatMap { attributeRecords =>
            val deleteActions = deleteEntityAttributes(attributeRecords)
            val deleteEntity = findEntityByName(workspaceContext.workspaceId, entityType, entityName).delete
            DBIO.seq(deleteActions:_*) andThen deleteEntity.map(_ > 0)
          }
      }
    }

    /** list all entities of the given type in the workspace */
    def list(workspaceContext: SlickWorkspaceContext, entityType: String): ReadAction[TraversableOnce[Entity]] = {
      // query gets entity records, all of its attributes (if there are any), and referred entities (if there are any)
      val entityAndAttributesQuery =
        findEntityByType(workspaceContext.workspaceId, entityType) joinLeft entityAttributesWithRefsJoin on (_.id === _._1._1.entityId)

      unmarshalEntities(entityAndAttributesQuery)
    }

    def rename(workspaceContext: SlickWorkspaceContext, entityType: String, oldName: String, newName: String): ReadWriteAction[Int] = {
      findEntityByName(workspaceContext.workspaceId, entityType, oldName).map(_.name).update(newName)
    }

    def getEntityTypes(workspaceContext: SlickWorkspaceContext): ReadAction[TraversableOnce[String]] = {
      filter(_.workspaceId === workspaceContext.workspaceId).map(_.entityType).distinct.result
    }

    def listEntitiesAllTypes(workspaceContext: SlickWorkspaceContext): ReadAction[TraversableOnce[Entity]] = {
      // query gets entity records, all of its attributes (if there are any), and referred entities (if there are any)
      val entityAndAttributesQuery =
        findEntityByWorkspace(workspaceContext.workspaceId) joinLeft entityAttributesWithRefsJoin on (_.id === _._1._1.entityId)

      unmarshalEntities(entityAndAttributesQuery)
    }

    def cloneAllEntities(sourceWorkspaceContext: SlickWorkspaceContext, destWorkspaceContext: SlickWorkspaceContext): ReadWriteAction[Unit] = {
      val allEntitiesAction = listEntitiesAllTypes(sourceWorkspaceContext)

      allEntitiesAction.flatMap(cloneEntities(destWorkspaceContext, _))
    }

    def cloneEntities(destWorkspaceContext: SlickWorkspaceContext, entities: TraversableOnce[Entity]): ReadWriteAction[Unit] = {
      //1. First save JUST the entity record for the entity without any attributes (references)
      val insertEntitiesOnly = DBIO.seq(entities.map(entity => save(destWorkspaceContext, entity.copy(attributes = Map.empty))).toSeq:_*)

      //2. Re-save the entities, but this time with the attributes that were previously omitted
      //Since all entities were already saved in step 1, we can now save the entities again with their references to other entities
      val insertEntitiesAndAttributes = DBIO.seq(entities.map(entity => save(destWorkspaceContext, entity)).toSeq:_*)

      insertEntitiesOnly andThen insertEntitiesAndAttributes
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
          val entityAndAttributesQuery = findEntityById(id) joinLeft entityAttributesWithRefsJoin on (_.id === _._1._1.entityId)
          unmarshalEntities(entityAndAttributesQuery)
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

    private def unmarshalEntity(entityRecord: EntityRecord, attributes: Map[String, Attribute]) = {
      Entity(entityRecord.name, entityRecord.entityType, attributes)
    }

    private def insertEntityAttributes(entity: Entity, entityId: Long, workspaceId: UUID): Seq[ReadWriteAction[Int]] = {
      val attributeInserts = entity.attributes.flatMap { case (name, attribute) =>
        attributeQuery.insertAttributeRecords(name, attribute, workspaceId)
      } map (_.flatMap { attributeId =>
        entityAttributeQuery += EntityAttributeRecord(entityId, attributeId)
      })
      attributeInserts.toSeq
    }

    private def deleteEntityAttributes(attributeRecords: Seq[AttributeRecord]) = {
      Seq(deleteEntityAttributeMappings(attributeRecords), attributeQuery.deleteAttributeRecords(attributeRecords))
    }

    private def deleteEntityAttributeMappings(attributeRecords: Seq[AttributeRecord]): WriteAction[Int] = {
      entityAttributeQuery.filter(_.attributeId inSet (attributeRecords.map(_.id))).delete
    }
  }
}
