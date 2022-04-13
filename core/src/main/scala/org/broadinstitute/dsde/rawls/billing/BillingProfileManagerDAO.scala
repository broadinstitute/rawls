package org.broadinstitute.dsde.rawls.billing

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.config.MultiCloudWorkspaceConfig
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.model.{CreationStatuses, RawlsBillingProject, RawlsBillingProjectName, SamResourceAction, SamResourceTypeNames, UserInfo, WorkspaceAzureCloudContext}

import scala.concurrent.{ExecutionContext, Future}

/**
 * Facade over the billing profile manager service. This service will eventually by the source of truth for
 * billing profiles in the Terra system. For now, we are using this to layer in "external" billing profiles
 * for the purposes of testing Azure workspaces.
 */
class BillingProfileManagerDAO(samDAO: SamDAO, config: MultiCloudWorkspaceConfig) extends LazyLogging {

  /**
   * Fetches the billing profiles to which the user has access
   */
  def listBillingProfiles(userInfo: UserInfo)(implicit ec: ExecutionContext): Future[Seq[RawlsBillingProject]] = {
    if (!config.multiCloudWorkspacesEnabled) {
      return Future.successful(Seq())
    }

    val azureConfig = config.azureConfig match {
      case None =>
        logger.warn("Multicloud workspaces enabled but no azure config setup, returning empty list of billng profiles")
        return Future.successful(Seq())
      case Some(value) => value
    }

    // NB until the BPM is live, we are returning a hardcoded
    // Azure billing profile, with access enforced by SAM
    samDAO.userHasAction(
      SamResourceTypeNames.managedGroup,
      azureConfig.alphaFeatureGroup,
      SamResourceAction("use"),
      userInfo
    ).flatMap {
      case true =>
        Future.successful(
          Seq(
            RawlsBillingProject(
              RawlsBillingProjectName(azureConfig.billingProjectName),
              CreationStatuses.Ready,
              None,
              None,
              azureManagedAppCoordinates = Some(
                WorkspaceAzureCloudContext(
                  azureConfig.azureTenantId,
                  azureConfig.azureSubscriptionId,
                  azureConfig.azureResourceGroupId
                )
              )
            )
          )
        )
      case false =>
        Future.successful(Seq.empty)
    }
  }

}
