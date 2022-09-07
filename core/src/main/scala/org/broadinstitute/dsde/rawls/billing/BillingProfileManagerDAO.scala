package org.broadinstitute.dsde.rawls.billing

import bio.terra.profile.model.{AzureManagedAppModel, CloudPlatform, CreateProfileRequest, ProfileModel}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.config.MultiCloudWorkspaceConfig
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.model.{
  AzureManagedAppCoordinates,
  CreationStatuses,
  ErrorReport,
  RawlsBillingAccountName,
  RawlsBillingProject,
  RawlsBillingProjectName,
  RawlsRequestContext,
  SamResourceAction,
  SamResourceTypeNames,
  UserInfo
}

import java.util.UUID
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

/**
 * Common interface for Billing Profile Manager operations
 */
trait BillingProfileManagerDAO {
  def createBillingProfile(displayName: String,
                           billingInfo: Either[RawlsBillingAccountName, AzureManagedAppCoordinates],
                           ctx: RawlsRequestContext
  ): Future[ProfileModel]

  def listManagedApps(subscriptionId: UUID, ctx: RawlsRequestContext): Future[Seq[AzureManagedAppModel]]

  def getBillingProfile(billingProfileId: UUID, ctx: RawlsRequestContext): Option[ProfileModel]

  def getAllBillingProfiles(ctx: RawlsRequestContext)(implicit ec: ExecutionContext): Future[Seq[ProfileModel]]

  // This is a temporary method that will be deleted once users can create their own Azure-backed billing projects in Terra.
  def getHardcodedAzureBillingProject(samUserResourceIds: Set[String], userInfo: UserInfo)(implicit
    ec: ExecutionContext
  ): Future[Seq[RawlsBillingProject]]

}

class ManagedAppNotFoundException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)

object BillingProfileManagerDAO {
  val BillingProfileRequestBatchSize = 1000
}

/**
 * Facade over the billing profile manager service. This service will eventually by the source of truth for
 * billing profiles in the Terra system. For now, we are using this to layer in "external" billing profiles
 * for the purposes of testing Azure workspaces.
 */
class BillingProfileManagerDAOImpl(
  samDAO: SamDAO,
  apiClientProvider: BillingProfileManagerClientProvider,
  config: MultiCloudWorkspaceConfig
) extends BillingProfileManagerDAO
    with LazyLogging {

  override def listManagedApps(subscriptionId: UUID, ctx: RawlsRequestContext): Future[Seq[AzureManagedAppModel]] = {
    val azureApi = apiClientProvider.getAzureApi(ctx)

    val result = azureApi.getManagedAppDeployments(subscriptionId).getManagedApps.asScala.toList
    Future.successful(result)
  }

  override def createBillingProfile(
    displayName: String,
    billingInfo: Either[RawlsBillingAccountName, AzureManagedAppCoordinates],
    ctx: RawlsRequestContext
  ): Future[ProfileModel] = {
    val azureManagedAppCoordinates = billingInfo match {
      case Left(_)       => throw new NotImplementedError("Google billing accounts not supported in billing profiles")
      case Right(coords) => coords
    }

    // create the profile
    val profileApi = apiClientProvider.getProfileApi(ctx)
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

  def getBillingProfile(billingProfileId: UUID, ctx: RawlsRequestContext): Option[ProfileModel] =
    Option(apiClientProvider.getProfileApi(ctx).getProfile(billingProfileId))

  def getAllBillingProfiles(ctx: RawlsRequestContext)(implicit ec: ExecutionContext): Future[Seq[ProfileModel]] = {

    if (!config.multiCloudWorkspacesEnabled) {
      return Future.successful(Seq())
    }

    val azureConfig = config.azureConfig match {
      case None =>
        logger.warn("Multicloud workspaces enabled but no azure config setup, returning empty list of billing profiles")
        return Future.successful(Seq())
      case Some(value) => value
    }

    val profileApi = apiClientProvider.getProfileApi(ctx)

    @tailrec
    def callListProfiles(accumulator: Seq[ProfileModel] = Seq.empty, offset: Int = 0): Seq[ProfileModel] = {
      val response = profileApi.listProfiles(offset, BillingProfileManagerDAO.BillingProfileRequestBatchSize)
      // if we got the entire batch size, call next batch
      if (response.getTotal >= BillingProfileManagerDAO.BillingProfileRequestBatchSize) {
        callListProfiles(accumulator ++ response.getItems.asScala.toSeq, offset + response.getTotal)
      } else {
        accumulator ++ response.getItems.asScala.toSeq
      }
    }

    // NB until the BPM is live, we want to ensure user is in the alpha group
    samDAO
      .userHasAction(
        SamResourceTypeNames.managedGroup,
        azureConfig.alphaFeatureGroup,
        SamResourceAction("use"),
        ctx.userInfo
      )
      .flatMap {
        case true => Future.successful(callListProfiles())
        case _    => Future.successful(Seq())
      }
  }

  def getHardcodedAzureBillingProject(samUserResourceIds: Set[String], userInfo: UserInfo)(implicit
    ec: ExecutionContext
  ): Future[Seq[RawlsBillingProject]] = {
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
      billingProjects <- samDAO
        .userHasAction(
          SamResourceTypeNames.managedGroup,
          azureConfig.alphaFeatureGroup,
          SamResourceAction("use"),
          userInfo
        )
        .flatMap {
          case true =>
            // Will remove after users can create Azure-backed Billing Accounts via Terra.
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
    } yield billingProjects.filter(bp => samUserResourceIds.contains(bp.projectName.value))
  }

}
