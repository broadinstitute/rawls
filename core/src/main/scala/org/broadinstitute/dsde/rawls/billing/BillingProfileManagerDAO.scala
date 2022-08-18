package org.broadinstitute.dsde.rawls.billing

import bio.terra.profile.model.{AzureManagedAppModel, CloudPlatform, CreateProfileRequest, ProfileModel}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.config.{AzureConfig, MultiCloudWorkspaceConfig}
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.model.{AzureManagedAppCoordinates, CreationStatuses, ErrorReport, RawlsBillingAccountName, RawlsBillingProject, RawlsBillingProjectName, SamResourceAction, SamResourceTypeNames, SamUserResource, UserInfo}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

/**
 * Common interface for Billing Profile Manager operations
 */
trait BillingProfileManagerDAO {
  def createBillingProfile(displayName: String, billingInfo: Either[RawlsBillingAccountName, AzureManagedAppCoordinates], userInfo: UserInfo): Future[ProfileModel]

  /***
    * Uses BPM to retrieve billing profile information for each of the RawlsBillingProjects (via their billingProjectIDs).
    */
  def populateBillingProfiles(samUserResources: Seq[SamUserResource], userInfo: UserInfo, rawlsBillingProjects: Seq[RawlsBillingProject])(implicit ec: ExecutionContext): Future[Seq[RawlsBillingProject]]

  def listManagedApps(subscriptionId: UUID, userInfo: UserInfo): Future[Seq[AzureManagedAppModel]]
}


class ManagedAppNotFoundException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)

/**
 * Facade over the billing profile manager service. This service will eventually by the source of truth for
 * billing profiles in the Terra system. For now, we are using this to layer in "external" billing profiles
 * for the purposes of testing Azure workspaces.
 */
class BillingProfileManagerDAOImpl(samDAO: SamDAO,
                                   apiClientProvider: BillingProfileManagerClientProvider,
                                   config: MultiCloudWorkspaceConfig) extends BillingProfileManagerDAO with LazyLogging {


  override def listManagedApps(subscriptionId: UUID, userInfo: UserInfo): Future[Seq[AzureManagedAppModel]] = {
    val azureApi = apiClientProvider.getAzureApi(userInfo.accessToken.token)

    val result = azureApi.getManagedAppDeployments(subscriptionId).getManagedApps.asScala.toList
    Future.successful(result)
  }

  override def createBillingProfile(displayName: String,
                                    billingInfo: Either[RawlsBillingAccountName, AzureManagedAppCoordinates],
                                    userInfo: UserInfo): Future[ProfileModel] = {
    val azureManagedAppCoordinates = billingInfo match {
      case Left(_) => throw new NotImplementedError("Google billing accounts not supported in billing profiles")
      case Right(coords) => coords
    }

    // create the profile
    val profileApi = apiClientProvider.getProfileApi(userInfo.accessToken.token)
    val createProfileRequest = new CreateProfileRequest()
      .tenantId(azureManagedAppCoordinates.tenantId)
      .subscriptionId(azureManagedAppCoordinates.subscriptionId)
      .managedResourceGroupId(azureManagedAppCoordinates.managedResourceGroupId)
      .displayName(displayName)
      .id(UUID.randomUUID())
      .biller("direct") // community terra is always 'direct' (i.e., no reseller)
      .cloudPlatform(CloudPlatform.AZURE)

    logger.info(s"Creating billing profile [id=${createProfileRequest.getId}]")
    val createdProfile = profileApi.createProfile(createProfileRequest)

    Future.successful(createdProfile)
  }


  /**
   * Fetches the details of the Azure billing profiles, based on the input rawlsBillingProjects.
   */
  def populateBillingProfiles(samUserResources: Seq[SamUserResource], userInfo: UserInfo, rawlsBillingProjects: Seq[RawlsBillingProject])(implicit ec: ExecutionContext): Future[Seq[RawlsBillingProject]] = {
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
      billingProfiles <- getAllBillingProfiles(azureConfig, userInfo, rawlsBillingProjects)
    } yield {
      billingProfiles.filter {
        bp => samUserResources.map(_.resourceId).contains(bp.projectName.value)
      }
    }
  }

  private def getAllBillingProfiles(azureConfig: AzureConfig, userInfo: UserInfo, rawlsBillingProjects: Seq[RawlsBillingProject])(implicit ec: ExecutionContext): Future[Seq[RawlsBillingProject]] = {
    // NB until the BPM is live, we are returning a hardcoded
    // Azure billing profile, with access enforced by SAM
    samDAO.userHasAction(
      SamResourceTypeNames.managedGroup,
      azureConfig.alphaFeatureGroup,
      SamResourceAction("use"),
      userInfo
    ).flatMap {
      case true =>
        val profileApi = apiClientProvider.getProfileApi(userInfo.accessToken.token)
        val fullBillingProjects = rawlsBillingProjects map {
          project =>
            // TODO: throw exception if ID doesn't exist (already filtering for this, so is it necessary?)
            val profileModel = profileApi.getProfile(UUID.fromString(project.billingProfileId.get))
            RawlsBillingProject(
              project.projectName,
              project.status,
              project.billingAccount,
              project.message,
              project.cromwellBackend,
              project.servicePerimeter,
              project.googleProjectNumber,
              project.invalidBillingAccount,
              project.spendReportDataset,
              project.spendReportTable,
              project.spendReportDatasetGoogleProject,
              azureManagedAppCoordinates = Some(
                AzureManagedAppCoordinates(
                  profileModel.getTenantId,
                  profileModel.getSubscriptionId,
                  profileModel.getManagedResourceGroupId
                )
              ),
              billingProfileId = project.billingProfileId
            )
        }

        // Will remove after users can create Azure-backed Billing Accounts via Terra.
        val temporaryAccount = Seq(RawlsBillingProject(
            RawlsBillingProjectName(azureConfig.billingProjectName),
            CreationStatuses.Ready,
            None,
            None,
            azureManagedAppCoordinates = Some(
              AzureManagedAppCoordinates(
                UUID.fromString(azureConfig.azureTenantId),
                UUID.fromString(azureConfig.azureSubscriptionId),
                azureConfig.azureResourceGroupId
              )
            )
          )
        )
        Future.successful(fullBillingProjects ++ temporaryAccount)

      case false =>
        Future.successful(Seq.empty)
    }
  }
}
