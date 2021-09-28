package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.util.UUID

import org.broadinstitute.dsde.rawls.model.AttributeName
import org.broadinstitute.dsde.rawls.model.{Workspace, _}
import slick.jdbc.{GetResult, JdbcProfile}

case class EntityAttributeStatisticsRecord(workspaceId: UUID, entityType: String, attributeNamespace: String, attributeName: String)

trait EntityAttributeStatisticsComponent {
  this: DriverComponent =>

  import driver.api._

  class EntityAttributeStatisticsTable(tag: Tag) extends Table[EntityAttributeStatisticsRecord](tag, "WORKSPACE_ENTITY_ATTRIBUTE_STATISTICS") {
    def workspaceId = column[UUID]("WORKSPACE_ID")
    def entityType = column[String]("ENTITY_TYPE", O.Length(254))
    def attributeNamespace = column[String]("ATTRIBUTE_NAMESPACE", O.Length(32))
    def attributeName = column[String]("ATTRIBUTE_NAME", O.Length(254))

    def * = (workspaceId, entityType, attributeNamespace, attributeName) <> (EntityAttributeStatisticsRecord.tupled, EntityAttributeStatisticsRecord.unapply)
  }

  object entityAttributeStatisticsQuery extends TableQuery(new EntityAttributeStatisticsTable(_)) {

    def batchInsert(workspaceId: UUID, typeWithNames: Map[String, Seq[AttributeName]]): WriteAction[Int] = {
      val records = typeWithNames.flatMap {  case (entityType, attributeNames) =>
        attributeNames.map { name =>
          EntityAttributeStatisticsRecord(workspaceId, entityType, name.namespace, name.name)
        }
      }

      (entityAttributeStatisticsQuery ++= records).map { result =>
        result.getOrElse(typeWithNames.size)
      }
    }

    def deleteAllForWorkspace(workspaceId: UUID): WriteAction[Int] = {
      entityAttributeStatisticsQuery.filter(_.workspaceId === workspaceId).delete
    }

    def getAll(workspaceId: UUID): ReadAction[Map[String, Seq[AttributeName]]] = {
      entityAttributeStatisticsQuery.filter(_.workspaceId === workspaceId).map { res =>
        (res.entityType, res.attributeNamespace, res.attributeName)
      }.result map { result =>
        result.groupBy(_._1).mapValues(_.map(x => AttributeName(x._2, x._3)))
      }
    }

    def checkAttributeNameExists(workspaceId: UUID, entityType: String, attributeNamespace: String, attributeName: String) = {
      EntityAttributeStatisticsRawSqlQuery.checkAttributeNameExists(workspaceId, entityType, attributeNamespace, attributeName)
    }

    private object EntityAttributeStatisticsRawSqlQuery extends RawSqlQuery {
      val driver: JdbcProfile = EntityAttributeStatisticsComponent.this.driver

      def checkAttributeNameExists(workspaceId: UUID, entityType: String, attributeNamespace: String, attributeName: String) = {
        sql"""select exists(select 1 from WORKSPACE_ENTITY_ATTRIBUTE_STATISTICS weas
              where weas.WORKSPACE_ID=$workspaceId and weas.ENTITY_TYPE=$entityType and weas.ATTRIBUTE_NAMESPACE=$attributeNamespace and weas.ATTRIBUTE_NAME=$attributeName)""".as[Boolean]
      }
    }

  }

}
