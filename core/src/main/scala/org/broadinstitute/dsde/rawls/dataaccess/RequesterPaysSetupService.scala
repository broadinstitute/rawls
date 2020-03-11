package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.{RawlsBillingProjectName, UserInfo, WorkspaceName}

import scala.concurrent.{ExecutionContext, Future}

class RequesterPaysSetupService(googleServicesDAO: GoogleServicesDAO, bondApiDAO: BondApiDAO, requesterPaysRole: String)(implicit executionContext: ExecutionContext) {

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

  def grantRequesterPaysToLinkedSAs(userInfo: UserInfo, workspaceName: WorkspaceName): Future[List[BondServiceAccountEmail]] = {
    for {
      emails <- getBondProviderServiceAccountEmails(userInfo)
      _ <- googleServicesDAO.addPolicyBindings(RawlsBillingProjectName(workspaceName.namespace), Map(requesterPaysRole -> emails.toSet.map{mail:BondServiceAccountEmail => "serviceAccount:" + mail.client_email}))
    } yield {
      emails
    }
  }

  def revokeRequesterPaysToLinkedSAs(userInfo: UserInfo, workspaceName: WorkspaceName): Future[List[BondServiceAccountEmail]] = {
    for {
      emails <- getBondProviderServiceAccountEmails(userInfo)
      _ <- googleServicesDAO.removePolicyBindings(RawlsBillingProjectName(workspaceName.namespace), Map(requesterPaysRole -> emails.toSet.map{mail:BondServiceAccountEmail => "serviceAccount:" + mail.client_email}))
    } yield {
      emails
    }
  }
}
