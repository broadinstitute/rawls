package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.model.AttributeName

import java.util.UUID

case class EntityAttributeStatisticsRecord(workspaceId: UUID,
                                           entityType: String,
                                           attributeNamespace: String,
                                           attributeName: String
)

trait EntityAttributeStatisticsComponent {
  this: DriverComponent =>

  import driver.api._

  class EntityAttributeStatisticsTable(tag: Tag)
      extends Table[EntityAttributeStatisticsRecord](tag, "WORKSPACE_ENTITY_ATTRIBUTE_STATISTICS") {
    def workspaceId = column[UUID]("WORKSPACE_ID")
    def entityType = column[String]("ENTITY_TYPE", O.Length(254))
    def attributeNamespace = column[String]("ATTRIBUTE_NAMESPACE", O.Length(32))
    def attributeName = column[String]("ATTRIBUTE_NAME", O.Length(254))

    def * = (workspaceId,
             entityType,
             attributeNamespace,
             attributeName
    ) <> (EntityAttributeStatisticsRecord.tupled, EntityAttributeStatisticsRecord.unapply)
  }

  object entityAttributeStatisticsQuery extends TableQuery(new EntityAttributeStatisticsTable(_)) {

    def batchInsert(workspaceId: UUID, typeWithNames: Map[String, Seq[AttributeName]]): WriteAction[Int] = {
      val records = typeWithNames.flatMap { case (entityType, attributeNames) =>
        attributeNames.map { name =>
          EntityAttributeStatisticsRecord(workspaceId, entityType, name.namespace, name.name)
        }
      }

      (entityAttributeStatisticsQuery ++= records).map { result =>
        result.getOrElse(typeWithNames.size)
      }
    }

    def deleteAllForWorkspace(workspaceId: UUID): WriteAction[Int] =
      entityAttributeStatisticsQuery.filter(_.workspaceId === workspaceId).delete

    def getAll(workspaceId: UUID): ReadAction[Map[String, Seq[AttributeName]]] =
      entityAttributeStatisticsQuery
        .filter(_.workspaceId === workspaceId)
        .map { res =>
          (res.entityType, res.attributeNamespace, res.attributeName)
        }
        .result map { result =>
        result.groupBy(_._1).view.mapValues(_.map(x => AttributeName(x._2, x._3))).toMap
      }

  }

}
