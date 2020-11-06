package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.{RawlsUserEmail, UserInfo, Workspace}

import scala.concurrent.{ExecutionContext, Future}

class RequesterPaysSetupService(dataSource: SlickDataSource, val googleServicesDAO: GoogleServicesDAO, val bondApiDAO: BondApiDAO, val requesterPaysRole: String)(implicit executionContext: ExecutionContext) {

  def getBondProviderServiceAccountEmails(userInfo: UserInfo): Future[List[BondServiceAccountEmail]] = {
    for {
      bondProviderList <- bondApiDAO.getBondProviders()
      bondResponses <- Future.traverse(bondProviderList) { provider =>
        bondApiDAO.getServiceAccountKey(provider, userInfo)
      }
    } yield {
      bondResponses collect {
        case Some(BondResponseData(email)) => email
      }
    }
  }

  def grantRequesterPaysToLinkedSAs(userInfo: UserInfo, workspace: Workspace): Future[List[BondServiceAccountEmail]] = {
    for {
      emails <- getBondProviderServiceAccountEmails(userInfo)
      _ <- googleServicesDAO.addPolicyBindings(workspace.googleProjectId, Map(requesterPaysRole -> emails.toSet.map{ mail:BondServiceAccountEmail => "serviceAccount:" + mail.client_email}))
      _ <- dataSource.inTransaction { dataAccess => dataAccess.workspaceRequesterPaysQuery.insertAllForUser(workspace.toWorkspaceName, userInfo.userEmail, emails.toSet) }
    } yield {
      emails
    }
  }

  def revokeUserFromWorkspace(userEmail: RawlsUserEmail, workspace: Workspace): Future[Seq[BondServiceAccountEmail]] = {
    for {
      emails <- dataSource.inTransaction { dataAccess => dataAccess.workspaceRequesterPaysQuery.listAllForUser(workspace.toWorkspaceName, userEmail) }
      _ <- revokeEmails(emails.toSet, userEmail, workspace)
    } yield emails.map(BondServiceAccountEmail)
  }

  private def revokeEmails(emails: Set[String], userEmail: RawlsUserEmail, workspace: Workspace): Future[Unit] = {
    for {
      keepBindings <- dataSource.inTransaction { dataAccess =>
        for {
          _ <- dataAccess.workspaceRequesterPaysQuery.deleteAllForUser(workspace.toWorkspaceName, userEmail)
          keepBindings <- dataAccess.workspaceRequesterPaysQuery.userExistsInWorkspaceNamespace(workspace.namespace, userEmail)
        } yield keepBindings
      }

      // only remove google bindings if there are no workspaces left in the namespace (i.e. project)
      _ <- if (keepBindings) {
        Future.successful(())
      } else {
        googleServicesDAO.removePolicyBindings(workspace.googleProjectId, Map(requesterPaysRole -> emails.map("serviceAccount:" + _)))
      }
    } yield ()
  }
}
