package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.stream.Materializer
import org.broadinstitute.dsde.rawls.model.{RawlsBillingProjectName, UserInfo}

import scala.concurrent.{ExecutionContext, Future}

class RequesterPaysSetupService(googleServicesDAO: GoogleServicesDAO, bondApiDAO: BondApiDAO, requesterPaysRole: String)(implicit val system: ActorSystem, val materializer: Materializer, val executionContext: ExecutionContext) {

  def getBondProviderServiceAccountEmails(userInfo: UserInfo): Future[List[BondServiceAccountEmail]] = {
    for {
      bondProviderList <- bondApiDAO.getBondProviders()
      bondResponses <- Future.traverse(bondProviderList) { provider => // todo: is traverse the simplest/best way to do this?
        bondApiDAO.getServiceAccountKey(provider, userInfo)
      }
    } yield {
      bondResponses collect {
        case Some(BondResponseData(email)) => email
      }
    }
  }

  def addBondProvidersToWorkspace(userInfo: UserInfo, rawlsBillingProjectName: RawlsBillingProjectName): Future[List[BondServiceAccountEmail]] = {
    for {
      emails <- getBondProviderServiceAccountEmails(userInfo)
      _ <- googleServicesDAO.addPolicyBindings(rawlsBillingProjectName, Map(requesterPaysRole -> emails.toSet.map("serviceAccount:"+_)))
    } yield {
      emails
    }
  }
}
