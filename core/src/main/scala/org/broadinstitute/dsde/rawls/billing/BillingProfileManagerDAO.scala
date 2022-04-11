package org.broadinstitute.dsde.rawls.billing

import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{CreationStatuses, RawlsBillingProject, RawlsBillingProjectName, SamResourceAction, SamResourceTypeNames, UserInfo}

import scala.concurrent.{ExecutionContext, Future}

class BillingProfileManagerDAO(samDAO: SamDAO) {

  def listBillingProfiles(userInfo: UserInfo)(implicit ec: ExecutionContext): Future[Seq[RawlsBillingProject]] = {
    samDAO.userHasAction(SamResourceTypeNames.managedGroup, "Alpha_Azure_Users", SamResourceAction("use"), userInfo).flatMap {
      case true =>
        Future.successful(
          Seq(
            RawlsBillingProject(
              RawlsBillingProjectName("alpha-azure-billing-project-20220407"),
              CreationStatuses.Ready,
              None,
              None
            )
          )
        )
      case false =>
        Future.successful(Seq.empty)
    }
  }

}
