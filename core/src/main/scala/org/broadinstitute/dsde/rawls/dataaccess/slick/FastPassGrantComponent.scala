package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.workbench.model.{WorkbenchEmail, WorkbenchUserId}
import org.broadinstitute.dsde.workbench.model.google.iam.{IamMemberTypes, IamResourceTypes}

import java.sql.Timestamp
import java.time.{Instant, ZoneOffset}
import java.util.UUID
import scala.language.postfixOps

/**
 * Created by tlangs on 3/16/2023.
 */
case class FastPassGrantRecord(
  id: Long,
  workspaceId: UUID,
  userSubjectId: String,
  accountEmail: String,
  accountType: String,
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
      fastPassGrant.accountEmail.value,
      fastPassGrant.accountType.value,
      fastPassGrant.resourceType.value,
      fastPassGrant.resourceName,
      fastPassGrant.organizationRole,
      new Timestamp(fastPassGrant.expiration.toInstant.toEpochMilli),
      new Timestamp(fastPassGrant.created.toInstant.toEpochMilli)
    )

  def toFastPassGrant(fastPassGrantRecord: FastPassGrantRecord): FastPassGrant =
    FastPassGrant(
      fastPassGrantRecord.id,
      fastPassGrantRecord.workspaceId.toString,
      WorkbenchUserId(fastPassGrantRecord.userSubjectId),
      WorkbenchEmail(fastPassGrantRecord.accountEmail),
      IamMemberTypes.withName(fastPassGrantRecord.accountType),
      IamResourceTypes.withName(fastPassGrantRecord.resourceType),
      fastPassGrantRecord.resourceName,
      fastPassGrantRecord.roleName,
      Instant.ofEpochMilli(fastPassGrantRecord.expiration.getTime).atZone(ZoneOffset.UTC).toOffsetDateTime,
      Instant.ofEpochMilli(fastPassGrantRecord.created.getTime).atZone(ZoneOffset.UTC).toOffsetDateTime
    )
}

trait FastPassGrantComponent {
  this: DriverComponent with WorkspaceComponent =>

  import driver.api._

  class FastPassGrantTable(tag: Tag) extends Table[FastPassGrantRecord](tag, "FASTPASS_GRANTS") {

    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

    def workspaceId = column[UUID]("workspace_id")

    def userSubjectId = column[String]("user_subject_id", O.Length(254))

    def accountEmail = column[String]("account_email", O.Length(254))

    def accountType = column[String]("account_type", O.Length(254))

    def resourceType = column[String]("resource_type", O.Length(254))

    def resourceName = column[String]("resource_name", O.Length(254))

    def roleName = column[String]("role_name", O.Length(254))

    def expiration = column[Timestamp]("expiration", O.SqlType("TIMESTAMP(6)"))

    def created = column[Timestamp]("created", O.SqlType("TIMESTAMP(6)"))

    def * = (id,
             workspaceId,
             userSubjectId,
             accountEmail,
             accountType,
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
    def findById(id: Long): ReadAction[Option[FastPassGrant]] =
      uniqueResult[FastPassGrantRecord](findByIdQuery(id)) flatMap {
        case None => DBIO.successful(None)
        case Some(fastPassGrantRecord) =>
          DBIO.successful(Option(FastPassGrantRecord.toFastPassGrant(fastPassGrantRecord)))
      }

    def findFastPassGrantsForWorkspace(workspaceId: UUID): ReadAction[Seq[FastPassGrant]] =
      loadFastPassGrants(findByWorkspaceIdQuery(workspaceId))

    def findFastPassGrantsForUser(userSubjectId: WorkbenchUserId): ReadAction[Seq[FastPassGrant]] =
      loadFastPassGrants(findByUserIdQuery(userSubjectId.value))

    def findFastPassGrantsForUserInWorkspace(workspaceId: UUID,
                                             userSubjectId: WorkbenchUserId
    ): ReadAction[Seq[FastPassGrant]] =
      loadFastPassGrants(findByWorkspaceAndUserQuery(workspaceId, userSubjectId.value))

    def findExpiredFastPassGrants(): ReadAction[Seq[FastPassGrant]] =
      loadFastPassGrants(findExpiredQuery)

    def insert(fastPassGrant: FastPassGrant): WriteAction[Long] =
      fastPassGrantQuery returning fastPassGrantQuery.map(_.id) += FastPassGrantRecord.fromFastPassGrant(fastPassGrant)

    def delete(id: Long): ReadWriteAction[Boolean] =
      findByIdQuery(id).delete.map(count => count > 0)

    def deleteMany(ids: Seq[Long]): ReadWriteAction[Boolean] =
      findByIdInQuery(ids).delete.map(count => count == ids.size)

    private def findByIdQuery(id: Long): FastPassGrantQueryType =
      filter(rec => rec.id === id)

    private def findByIdInQuery(ids: Seq[Long]): FastPassGrantQueryType =
      filter(rec => rec.id inSet ids)
    private def findByWorkspaceIdQuery(workspaceId: UUID): FastPassGrantQueryType =
      filter(rec => rec.workspaceId === workspaceId)

    private def findByUserIdQuery(userSubjectId: String): FastPassGrantQueryType =
      filter(rec => rec.userSubjectId === userSubjectId)

    private def findByWorkspaceAndUserQuery(workspaceId: UUID, userSubjectId: String): FastPassGrantQueryType =
      filter(rec => rec.workspaceId === workspaceId && rec.userSubjectId === userSubjectId)

    private def findExpiredQuery: FastPassGrantQueryType =
      filter(rec => rec.expiration < nowTimestamp)

    private def loadFastPassGrants(lookup: FastPassGrantQueryType): ReadAction[Seq[FastPassGrant]] =
      for {
        fastPassGrantRecords <- lookup.result
      } yield fastPassGrantRecords.map { fastPassGrantRecord =>
        FastPassGrantRecord.toFastPassGrant(fastPassGrantRecord)
      }
  }

}
