package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.model._

import java.sql.Timestamp
import java.util.UUID
import scala.language.postfixOps

/**
 * Created by tlangs on 3/16/2023.
 */
case class FastPassGrantRecord(
  id: UUID,
  workspaceId: UUID,
  userId: String,
  resourceType: String,
  resourceName: String,
  roleName: String,
  expiration: Timestamp
)

object FastPassGrantRecord {
  def fromFastPassGrant(fastPassGrant: FastPassGrant): FastPassGrantRecord =
    FastPassGrantRecord(
      UUID.fromString(fastPassGrant.id),
      UUID.fromString(fastPassGrant.workspaceId),
      fastPassGrant.userId.value,
      GcpResourceTypes.toName(fastPassGrant.resourceType),
      fastPassGrant.resourceName,
      IamRoles.toName(fastPassGrant.roleName),
      fastPassGrant.expiration
    )

  def toFastPassGrant(fastPassGrantRecord: FastPassGrantRecord): FastPassGrant =
    FastPassGrant(
      fastPassGrantRecord.id.toString,
      fastPassGrantRecord.workspaceId.toString,
      RawlsUserSubjectId(fastPassGrantRecord.userId),
      GcpResourceTypes.withName(fastPassGrantRecord.resourceType),
      fastPassGrantRecord.resourceName,
      IamRoles.withName(fastPassGrantRecord.roleName),
      fastPassGrantRecord.expiration
    )
}

trait FastPassGrantComponent {
  this: DriverComponent with WorkspaceComponent =>

  import driver.api._

  class FastPassGrantTable(tag: Tag) extends Table[FastPassGrantRecord](tag, "FASTPASS_GRANT") {

    def id = column[UUID]("id", O.PrimaryKey)

    def workspaceId = column[UUID]("workspace_id")

    def userId = column[String]("user_id", O.PrimaryKey)

    def resourceType = column[String]("resource_type", O.Length(254))

    def resourceName = column[String]("resource_name", O.Length(254))

    def roleName = column[String]("role_name", O.Length(254))

    def expiration = column[Timestamp]("expiration", O.SqlType("TIMESTAMP(6)"))

    def * = (id,
             workspaceId,
             userId,
             resourceType,
             resourceName,
             roleName,
             expiration
    ) <> ((FastPassGrantRecord.apply _).tupled, FastPassGrantRecord.unapply)

    def workspace = foreignKey("FK_WS_FPG", workspaceId, workspaceQuery)(_.id)

  }

  type FastPassGrantQueryType = driver.api.Query[FastPassGrantTable, FastPassGrantRecord, Seq]

  object fastPassGrantQuery extends TableQuery(new FastPassGrantTable(_)) {

    def listAll(): ReadAction[Seq[FastPassGrant]] =
      loadFastPassGrants(fastPassGrantQuery)

    def loadFastPassGrant(fastPassGrantId: UUID): ReadAction[Option[FastPassGrant]] =
      uniqueResult[FastPassGrantRecord](findById(fastPassGrantId)) flatMap {
        case None => DBIO.successful(None)
        case Some(fastPassGrantRecord) =>
          DBIO.successful(Option(FastPassGrantRecord.toFastPassGrant(fastPassGrantRecord)))
      }

    def findById(fastPassGrantId: UUID): FastPassGrantQueryType =
      filter(rec => rec.id === fastPassGrantId)

    def findByWorkspaceId(workspaceId: UUID): FastPassGrantQueryType =
      filter(rec => rec.workspaceId === workspaceId)

    def findByUserId(userId: String): FastPassGrantQueryType =
      filter(rec => rec.userId === userId)

    def findByWorkspaceAndUser(workspaceId: UUID, userId: String): FastPassGrantQueryType =
      filter(rec => rec.workspaceId === workspaceId && rec.userId === userId)

    private def loadFastPassGrants(lookup: FastPassGrantQueryType): ReadAction[Seq[FastPassGrant]] =
      for {
        fastPassGrantRecords <- lookup.result
      } yield fastPassGrantRecords.map(fastPassGrantRecord => FastPassGrantRecord.toFastPassGrant(fastPassGrantRecord))
  }

}
