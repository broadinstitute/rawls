package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.model._

import java.sql.Timestamp
import java.util.UUID
import scala.language.postfixOps

/**
 * Created by tlangs on 3/16/2023.
 */
case class FastPassGrantRecord(
  id: Long,
  workspaceId: UUID,
  userSubjectId: String,
  resourceType: String,
  resourceName: String,
  roleName: String,
  expiration: Timestamp,
  created: Timestamp
)

object FastPassGrantRecord {
  def fromFastPassGrant(fastPassGrant: FastPassGrant): FastPassGrantRecord =
    FastPassGrantRecord(
      fastPassGrant.id,
      UUID.fromString(fastPassGrant.workspaceId),
      fastPassGrant.userSubjectId.value,
      GcpResourceTypes.toName(fastPassGrant.resourceType),
      fastPassGrant.resourceName,
      IamRoles.toName(fastPassGrant.roleName),
      fastPassGrant.expiration,
      fastPassGrant.created
    )

  def toFastPassGrant(fastPassGrantRecord: FastPassGrantRecord): FastPassGrant =
    FastPassGrant(
      fastPassGrantRecord.id,
      fastPassGrantRecord.workspaceId.toString,
      RawlsUserSubjectId(fastPassGrantRecord.userSubjectId),
      GcpResourceTypes.withName(fastPassGrantRecord.resourceType),
      fastPassGrantRecord.resourceName,
      IamRoles.withName(fastPassGrantRecord.roleName),
      fastPassGrantRecord.expiration,
      fastPassGrantRecord.created
    )
}

trait FastPassGrantComponent {
  this: DriverComponent with WorkspaceComponent =>

  import driver.api._

  class FastPassGrantTable(tag: Tag) extends Table[FastPassGrantRecord](tag, "FASTPASS_GRANT") {

    def id = column[Long]("id", O.PrimaryKey)

    def workspaceId = column[UUID]("workspace_id")

    def userSubjectId = column[String]("user_subject_id", O.PrimaryKey)

    def resourceType = column[String]("resource_type", O.Length(254))

    def resourceName = column[String]("resource_name", O.Length(254))

    def roleName = column[String]("role_name", O.Length(254))

    def expiration = column[Timestamp]("expiration", O.SqlType("TIMESTAMP(6)"))

    def created = column[Timestamp]("created", O.SqlType("TIMESTAMP(6)"))

    def * = (id,
             workspaceId,
             userSubjectId,
             resourceType,
             resourceName,
             roleName,
             expiration,
             created
    ) <> ((FastPassGrantRecord.apply _).tupled, FastPassGrantRecord.unapply)

    def workspace = foreignKey("FK_WS_FPG", workspaceId, workspaceQuery)(_.id)

  }

  type FastPassGrantQueryType = driver.api.Query[FastPassGrantTable, FastPassGrantRecord, Seq]

  object fastPassGrantQuery extends TableQuery(new FastPassGrantTable(_)) {

    def listAll(): ReadAction[Seq[FastPassGrant]] =
      loadFastPassGrants(fastPassGrantQuery)

    def loadFastPassGrant(id: Long): ReadAction[Option[FastPassGrant]] =
      uniqueResult[FastPassGrantRecord](findById(id)) flatMap {
        case None => DBIO.successful(None)
        case Some(fastPassGrantRecord) =>
          DBIO.successful(Option(FastPassGrantRecord.toFastPassGrant(fastPassGrantRecord)))
      }

    def findById(id: Long): FastPassGrantQueryType =
      filter(rec => rec.id === id)

    def findByWorkspaceId(workspaceId: UUID): FastPassGrantQueryType =
      filter(rec => rec.workspaceId === workspaceId)

    def findByUserId(userSubjectId: String): FastPassGrantQueryType =
      filter(rec => rec.userSubjectId === userSubjectId)

    def findByWorkspaceAndUser(workspaceId: UUID, userSubjectId: String): FastPassGrantQueryType =
      filter(rec => rec.workspaceId === workspaceId && rec.userSubjectId === userSubjectId)

    private def loadFastPassGrants(lookup: FastPassGrantQueryType): ReadAction[Seq[FastPassGrant]] =
      for {
        fastPassGrantRecords <- lookup.result
      } yield fastPassGrantRecords.map(fastPassGrantRecord => FastPassGrantRecord.toFastPassGrant(fastPassGrantRecord))
  }

}
