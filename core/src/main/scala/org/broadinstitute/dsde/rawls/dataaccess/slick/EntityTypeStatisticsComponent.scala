package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.util.UUID

case class EntityTypeStatisticsRecord(workspaceId: UUID, entityType: String, entityCount: Long)

trait EntityTypeStatisticsComponent {
  this: DriverComponent =>

  import driver.api._

  class EntityTypeStatisticsTable(tag: Tag)
      extends Table[EntityTypeStatisticsRecord](tag, "WORKSPACE_ENTITY_TYPE_STATISTICS") {
    def workspaceId = column[UUID]("WORKSPACE_ID")
    def entityType = column[String]("ENTITY_TYPE", O.Length(254))
    def entityCount = column[Long]("ENTITY_TYPE_COUNT")

    def * =
      (workspaceId, entityType, entityCount) <> (EntityTypeStatisticsRecord.tupled, EntityTypeStatisticsRecord.unapply)
  }

  object entityTypeStatisticsQuery extends TableQuery(new EntityTypeStatisticsTable(_)) {

    def batchInsert(workspaceId: UUID, counts: Map[String, Int]): WriteAction[Int] = {
      val records = counts.map { case (entityType, entityCount) =>
        EntityTypeStatisticsRecord(workspaceId, entityType, entityCount)
      }.toSet

      (entityTypeStatisticsQuery ++= records).map { result =>
        result.getOrElse(counts.size)
      }
    }

    def deleteAllForWorkspace(workspaceId: UUID): WriteAction[Int] =
      entityTypeStatisticsQuery.filter(_.workspaceId === workspaceId).delete

    def getAll(workspaceId: UUID): ReadAction[Map[String, Int]] =
      entityTypeStatisticsQuery
        .filter(_.workspaceId === workspaceId)
        .map { res =>
          (res.entityType, res.entityCount)
        }
        .result map { result =>
        result.map { case (entityType, entityCount) =>
          (entityType, entityCount.toInt)
        }.toMap
      }

  }

}
