package org.broadinstitute.dsde.rawls.billing

import scala.jdk.CollectionConverters._
import akka.http.scaladsl.model.StatusCodes
import bio.terra.profile.api.{AzureApi, ProfileApi}
import bio.terra.profile.client.ApiClient
import bio.terra.profile.model.{AzureManagedAppModel, AzureManagedAppsResponseModel, CloudPlatform, CreateProfileRequest, ProfileModel}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.config.{AzureConfig, MultiCloudWorkspaceConfig}
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.model.{AzureManagedAppCoordinates, CreationStatuses, ErrorReport, RawlsBillingAccountName, RawlsBillingProject, RawlsBillingProjectName, RawlsBillingProjectResponse, SamResourceAction, SamResourceTypeNames, SamUserResource, UserInfo}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.util.{Failure, Success, Try}

trait BillingProfileManagerDAO {
  def listManagedApps(subscriptionId: UUID, userInfo: UserInfo): Future[Seq[AzureManagedAppModel]]

  def listBillingProfiles(userInfo: UserInfo, samUserResources: Seq[SamUserResource])(implicit ec: ExecutionContext): Future[Seq[RawlsBillingProject]]

  def createBillingProfile(displayName: String, billingInfo: Either[RawlsBillingAccountName, AzureManagedAppCoordinates], userInfo: UserInfo): Future[ProfileModel]

}


trait BillingProfileManagerClientProvider {
  def getApiClient(accessToken: String): ApiClient

  def getAzureApi(accessToken: String): AzureApi

  def getProfileApi(accessToken: String): ProfileApi
}

class HttpBillingProfileManagerClientProvider(baseBpmUrl: String) extends BillingProfileManagerClientProvider {
  override def getApiClient(accessToken: String): ApiClient = {
    val client: ApiClient = new ApiClient()
    client.setBasePath(baseBpmUrl)
    client.setAccessToken(accessToken)

    client
  }

  override def getAzureApi(accessToken: String): AzureApi = {
    new AzureApi(getApiClient(accessToken))
  }

  override def getProfileApi(accessToken: String): ProfileApi = {
    new ProfileApi(getApiClient(accessToken))
  }
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

    val result = blocking {
      azureApi.getManagedAppDeployments(subscriptionId)
    }.getManagedApps.asScala.toList
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
      .displayName(displayName)
      .applicationDeploymentName("FAKE")
      .id(UUID.randomUUID())
      .biller("direct") // community terra is always 'direct' (i.e., no reseller)
      .cloudPlatform(CloudPlatform.AZURE)
      .resourceGroupName("FAKE")

    logger.info(s"Creating billing profile [id=${createProfileRequest.getId}]")
    val createdProfile = blocking {
      profileApi.createProfile(createProfileRequest)
    }

    Future.successful(createdProfile)
  }


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
      billingProfiles.filter {
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
                  UUID.fromString(azureConfig.azureTenantId),
                  UUID.fromString(azureConfig.azureSubscriptionId),
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
