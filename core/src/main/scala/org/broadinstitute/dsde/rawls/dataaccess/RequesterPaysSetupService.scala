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
      keepBindings <- dataSource.inTransaction { dataAccess =>
        for {
          _ <- dataAccess.workspaceRequesterPaysQuery.deleteAllForUser(workspace.toWorkspaceName, userEmail)
          // TODO (CA-1236): Remove after PPW migration is complete, we won't need to track on workspace namespace anymore, since google project will be per workspace
          keepBindings <- dataAccess.workspaceRequesterPaysQuery.userExistsInWorkspaceNamespaceAssociatedGoogleProject(
            workspace.namespace,
            userEmail
          )
        } yield keepBindings
      }
      // TODO (CA-1236): Clean up the if statement once PPW migration is complete. There will be no more V1 workspaces so this will be dead code
      // only remove google bindings if there are no workspaces left in the namespace (i.e. project) or the workspace is V2
      _ <-
        if (keepBindings) {
          Future.successful(())
        } else {
          googleServicesDAO.removePolicyBindings(
            workspace.googleProjectId,
            Map(requesterPaysRole -> emails.map("serviceAccount:" + _.client_email))
          )
        }
    } yield ()
}
