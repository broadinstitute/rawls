package org.broadinstitute.dsde.rawls.dataaccess

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import org.broadinstitute.dsde.rawls.model.{RawlsUserEmail, UserInfo, Workspace}

import scala.concurrent.{ExecutionContext, Future}

class RequesterPaysSetupService(dataSource: SlickDataSource,
                                val googleServicesDAO: GoogleServicesDAO,
                                val bondApiDAO: BondApiDAO,
                                val requesterPaysRole: String
)(implicit executionContext: ExecutionContext) {

  def getBondProviderServiceAccountEmails(userInfo: UserInfo): Future[List[BondServiceAccountEmail]] =
    for {
      bondProviderList <- bondApiDAO.getBondProviders()
      bondResponses <- Future.traverse(bondProviderList) { provider =>
        bondApiDAO.getServiceAccountKey(provider, userInfo)
      }
    } yield bondResponses collect { case Some(BondResponseData(email)) =>
      email
    }

  def grantRequesterPaysToLinkedSAs(userInfo: UserInfo, workspace: Workspace): Future[List[BondServiceAccountEmail]] =
    for {
      emails <- getBondProviderServiceAccountEmails(userInfo)
      _ <- googleServicesDAO.addPolicyBindings(workspace.googleProjectId,
                                               Map(requesterPaysRole -> emails.toSet.map {
                                                 mail: BondServiceAccountEmail => "serviceAccount:" + mail.client_email
                                               })
      )
      _ <- dataSource.inTransaction { dataAccess =>
        dataAccess.workspaceRequesterPaysQuery.insertAllForUser(workspace.toWorkspaceName,
                                                                userInfo.userEmail,
                                                                emails.toSet
        )
      }
    } yield emails

  def revokeUserFromWorkspace(userEmail: RawlsUserEmail, workspace: Workspace): Future[Seq[BondServiceAccountEmail]] =
    for {
      emails <- dataSource.inTransaction { dataAccess =>
        dataAccess.workspaceRequesterPaysQuery.listAllForUser(workspace.toWorkspaceName, userEmail)
      }
      _ <- revokeEmails(emails.toSet, userEmail, workspace)
    } yield emails

  def revokeAllUsersFromWorkspace(workspace: Workspace): Future[Seq[BondServiceAccountEmail]] =
    for {
      userEmailsToSAEmail <- dataSource.inTransaction { dataAccess =>
        dataAccess.workspaceRequesterPaysQuery.listAllForWorkspace(workspace.toWorkspaceName)
      }
      _ <- userEmailsToSAEmail.toList
        .traverse { case (userEmail, saEmails) =>
          IO.fromFuture(IO(revokeEmails(saEmails.toSet, userEmail, workspace)))
        }
        .unsafeToFuture()
    } yield userEmailsToSAEmail.flatMap(_._2).toSeq

  private def revokeEmails(emails: Set[BondServiceAccountEmail],
                           userEmail: RawlsUserEmail,
                           workspace: Workspace
  ): Future[Unit] =
    for {
      _ <- dataSource.inTransaction { dataAccess =>
        dataAccess.workspaceRequesterPaysQuery.deleteAllForUser(workspace.toWorkspaceName, userEmail)
      }
      _ <- googleServicesDAO.removePolicyBindings(
            workspace.googleProjectId,
            Map(requesterPaysRole -> emails.map("serviceAccount:" + _.client_email))
          )
    } yield ()
}
