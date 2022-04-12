package org.broadinstitute.dsde.rawls.billing

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.config.MultiCloudWorkspaceConfig
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.model.{CreationStatuses, RawlsBillingProject, RawlsBillingProjectName, SamResourceAction, SamResourceTypeNames, UserInfo}

import scala.concurrent.{ExecutionContext, Future}

/**
 * Facade over the billing profile manager service. This service will eventually by the source of truth for
 * billing profiles in the Terra system.
 */
class BillingProfileManagerDAO(samDAO: SamDAO, config: MultiCloudWorkspaceConfig) {

  /**
   * Fetches the billing profiles this user has access to
   */
  def listBillingProfiles(userInfo: UserInfo)(implicit ec: ExecutionContext): Future[Seq[RawlsBillingProject]] = {
    // NB until the BPM is live, we are returning a hardcoded
    // Azure billing profile, with access enforced by SAM
    if (!config.multiCloudWorkspacesEnabled) {
      return Future.successful(Seq())
    }

    val azureConfig =  config.azureConfig.getOrElse(
      throw new RawlsException("Invalid multicloud configuration")
    )

    samDAO.userHasAction(SamResourceTypeNames.managedGroup, azureConfig.alphaFeatureGroup, SamResourceAction("use"), userInfo).flatMap {
      case true =>
        Future.successful(
          Seq(
            RawlsBillingProject(
              RawlsBillingProjectName(azureConfig.billingProjectName),
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
