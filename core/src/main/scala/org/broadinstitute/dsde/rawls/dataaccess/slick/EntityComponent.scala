package org.broadinstitute.dsde.rawls.dataaccess.slick

import com.google.common.annotations.VisibleForTesting
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.model.{Workspace, _}
import slick.jdbc.{GetResult, JdbcProfile}

import java.sql.Timestamp
import java.util.UUID
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
}

case class EntityRecord(id: Long,
                        name: String,
                        entityType: String,
                        workspaceId: UUID,
                        recordVersion: Long,
                        deleted: Boolean,
                        deletedDate: Option[Timestamp]
) extends EntityRecordBase

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
}

class EntityTable(tag: Tag) extends EntityTableBase[EntityRecord](tag) {
  def * =
    (id, name, entityType, workspaceId, version, deleted, deletedDate) <> (EntityRecord.tupled, EntityRecord.unapply)
}

//noinspection TypeAnnotation
trait EntityComponent {
  this: DriverComponent =>

  object entityQuery extends TableQuery(new EntityTable(_)) with LazyLogging {

    type EntityQuery = Query[EntityTable, EntityRecord, Seq]

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
    }

    // Slick queries

    // Active queries: only return entities with their deleted flag set to false

    @VisibleForTesting
    def findActiveEntityByWorkspace(workspaceId: UUID): EntityQuery =
      filter(entRec => entRec.workspaceId === workspaceId && !entRec.deleted)

    // queries which may include "deleted" hidden entities

    def findEntityByName(workspaceId: UUID, entityType: String, entityName: String): EntityQuery =
      filter(entRec =>
        entRec.name === entityName && entRec.entityType === entityType && entRec.workspaceId === workspaceId
      )

    def findEntityById(id: Long): EntityQuery =
      filter(_.id === id)

    // Actions

    def getEntityRecords(workspaceId: UUID, entities: Set[AttributeEntityReference]): ReadAction[Seq[EntityRecord]] = {
      val entitiesGrouped = entities.grouped(batchSize).toSeq

      DBIO
        .sequence(entitiesGrouped map { batch =>
          EntityRecordRawSqlQuery.action(workspaceId, batch)
        })
        .map(_.flatten)
    }

    // perform actual deletion (not hiding) of all entities in a workspace
    def deleteFromDb(workspaceContext: Workspace): WriteAction[Int] =
      filter(_.workspaceId === workspaceContext.workspaceIdAsUUID).delete
  }

}
