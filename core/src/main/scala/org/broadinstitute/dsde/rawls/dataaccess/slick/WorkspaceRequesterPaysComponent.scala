package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.util.UUID

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.BondServiceAccountEmail
import org.broadinstitute.dsde.rawls.model.{RawlsUserEmail, WorkspaceName}

case class WorkspaceRequesterPaysRecord(id: Long, workspaceId: UUID, userEmail: String, serviceAccountEmail: String)

trait WorkspaceRequesterPaysComponent {
  this: DriverComponent with WorkspaceComponent =>

  import driver.api._

  class WorkspaceRequesterPaysTable(tag: Tag) extends Table[WorkspaceRequesterPaysRecord](tag, "WORKSPACE_REQUESTER_PAYS") {
    def id = column[Long]("ID", O.PrimaryKey)
    def workspaceId = column[UUID]("WORKSPACE_ID")
    def userEmail = column[String]("USER_EMAIL", O.Length(254))
    def serviceAccountEmail = column[String]("SERVICE_ACCOUNT_EMAIL", O.Length(254))

    def * = (id, workspaceId, userEmail, serviceAccountEmail) <> (WorkspaceRequesterPaysRecord.tupled, WorkspaceRequesterPaysRecord.unapply)
  }

  object workspaceRequesterPaysQuery extends TableQuery(new WorkspaceRequesterPaysTable(_)) {
    def insertAllForUser(workspaceName: WorkspaceName, userEmail: RawlsUserEmail, serviceAccountEmails: Set[BondServiceAccountEmail]): ReadWriteAction[Int] = {
      for {
        maybeWorkspaceId <- workspaceQuery.getWorkspaceId(workspaceName)
        workspaceId = maybeWorkspaceId.getOrElse(throw new RawlsException(s"workspace not found $workspaceName"))
        existingRecords <- existingRecordsForUserQuery(workspaceName, userEmail).result
        saEmailsToInsert = serviceAccountEmails.map(_.client_email) -- existingRecords.map(_.serviceAccountEmail)
        results <- workspaceRequesterPaysQuery ++= saEmailsToInsert.map(saEmail => WorkspaceRequesterPaysRecord(0, workspaceId, userEmail.value, saEmail))
      } yield results.getOrElse(saEmailsToInsert.size) // results seems to be None when there are multiple inserts :(
    }

    def deleteAllForUser(workspaceName: WorkspaceName, userEmail: RawlsUserEmail): ReadWriteAction[Int] = {
      existingRecordsForUserQuery(workspaceName, userEmail).delete
    }

    def userExistsInWorkspaceNamespace(namespace: String, userEmail: RawlsUserEmail): ReadAction[Boolean] = {
      val query = (workspaceQuery join workspaceRequesterPaysQuery on (_.id === _.workspaceId)).filter { case (ws, rp) =>
          ws.namespace === namespace && rp.userEmail === userEmail.value
      }

      query.exists.result
    }

    def listAllForUser(workspaceName: WorkspaceName, userEmail: RawlsUserEmail): ReadAction[Seq[String]] = {
      existingRecordsForUserQuery(workspaceName, userEmail).map(_.serviceAccountEmail).result
    }
  }

  private def existingRecordsForUserQuery(workspaceName: WorkspaceName, userEmail: RawlsUserEmail): Query[WorkspaceRequesterPaysTable, WorkspaceRequesterPaysRecord, Seq] = {
    val workspaceSubquery = workspaceQuery.filter(ws => ws.namespace === workspaceName.namespace && ws.name === workspaceName.name).map(_.id)
    workspaceRequesterPaysQuery.filter(_.workspaceId in workspaceSubquery).filter(_.userEmail === userEmail.value)
  }
}
