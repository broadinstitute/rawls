package org.broadinstitute.dsde.rawls.billing

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.config.{AzureConfig, MultiCloudWorkspaceConfig}
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.model.{CreationStatuses, RawlsBillingProject, RawlsBillingProjectName, RawlsBillingProjectResponse, SamResourceAction, SamResourceTypeNames, SamUserResource, UserInfo, AzureManagedAppCoordinates}

import scala.concurrent.{ExecutionContext, Future}

trait BillingProfileManagerDAO {
  def listBillingProfiles(userInfo: UserInfo, samUserResources: Seq[SamUserResource])(implicit ec: ExecutionContext): Future[Seq[RawlsBillingProject]]
}


/**
 * Facade over the billing profile manager service. This service will eventually by the source of truth for
 * billing profiles in the Terra system. For now, we are using this to layer in "external" billing profiles
 * for the purposes of testing Azure workspaces.
 */
class BillingProfileManagerDAOImpl(samDAO: SamDAO, config: MultiCloudWorkspaceConfig) extends BillingProfileManagerDAO with LazyLogging {

  /**
   * Fetches the billing profiles to which the user has access.
   *
   * This method only returns Azure billing profiles for now
   */
  def listBillingProfiles(userInfo: UserInfo, samUserResources: Seq[SamUserResource])(implicit ec: ExecutionContext): Future[Seq[RawlsBillingProject]] = {
    if (!config.multiCloudWorkspacesEnabled) {
      return Future.successful(Seq())
    }

    val azureConfig = config.azureConfig match {
      case None =>
        logger.warn("Multicloud workspaces enabled but no azure config setup, returning empty list of billing profiles")
        return Future.successful(Seq())
      case Some(value) => value
    }

    for {
      billingProfiles <- getAllBillingProfiles(azureConfig, userInfo)
    } yield {
      billingProfiles.filter  {
        bp => samUserResources.map(_.resourceId).contains(bp.projectName.value)
      }
    }
  }


  private def getAllBillingProfiles(azureConfig: AzureConfig, userInfo: UserInfo)(implicit ec: ExecutionContext): Future[Seq[RawlsBillingProject]] = {
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
                AzureManagedAppCoordinates(
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
