package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.StatusCodes
import bio.terra.profile.client.ApiException
import bio.terra.profile.model._
import com.typesafe.scalalogging.LazyLogging
import org.apache.http.HttpStatus
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.billing.BillingProfileManagerDAO.ProfilePolicy.ProfilePolicy
import org.broadinstitute.dsde.rawls.config.MultiCloudWorkspaceConfig
import org.broadinstitute.dsde.rawls.model.ProjectRoles.ProjectRole
import org.broadinstitute.dsde.rawls.model.{
  AzureManagedAppCoordinates,
  ErrorReport,
  ProjectRoles,
  RawlsBillingAccountName,
  RawlsRequestContext
}
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import java.util.{Date, UUID}
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

case class BpmAzureReportErrorMessage(message: String, statusCode: Int)

object BpmAzureReportErrorMessageJsonProtocol extends DefaultJsonProtocol {
  implicit val bpmAzureReportErrorMessageFormat: RootJsonFormat[BpmAzureReportErrorMessage] = jsonFormat2(
    BpmAzureReportErrorMessage.apply
  )
}

import spray.json._
import BpmAzureReportErrorMessageJsonProtocol._

/**
 * Common interface for Billing Profile Manager operations
 */
trait BillingProfileManagerDAO {
  def createBillingProfile(displayName: String,
                           billingInfo: Either[RawlsBillingAccountName, AzureManagedAppCoordinates],
                           policies: Map[String, List[(String, String)]],
                           ctx: RawlsRequestContext
  ): ProfileModel

  def deleteBillingProfile(billingProfileId: UUID, ctx: RawlsRequestContext): Unit

  def listManagedApps(subscriptionId: UUID,
                      includeAssignedApps: Boolean,
                      ctx: RawlsRequestContext
  ): Seq[AzureManagedAppModel]

  def getBillingProfile(billingProfileId: UUID, ctx: RawlsRequestContext): Option[ProfileModel]

  def getAllBillingProfiles(ctx: RawlsRequestContext)(implicit ec: ExecutionContext): Future[Seq[ProfileModel]]

  def addProfilePolicyMember(billingProfileId: UUID,
                             policy: ProfilePolicy,
                             memberEmail: String,
                             ctx: RawlsRequestContext
  ): Unit

  def deleteProfilePolicyMember(billingProfileId: UUID,
                                policy: ProfilePolicy,
                                memberEmail: String,
                                ctx: RawlsRequestContext
  ): Unit

  def getStatus(): SystemStatus

  @throws(classOf[BpmAzureSpendReportApiException])
  def getAzureSpendReport(billingProfileId: UUID,
                          spendReportStartDate: Date,
                          spendReportEndDate: Date,
                          ctx: RawlsRequestContext
  ): SpendReport
}

class ManagedAppNotFoundException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)

class BpmAzureSpendReportApiException(val statusCode: Int, message: String, cause: Throwable = null)
    extends Exception(message, cause)

class BpmException(billingProfileId: UUID, cause: Throwable)
    extends RawlsExceptionWithErrorReport(
      ErrorReport(StatusCodes.InternalServerError, s"Error fetching billing profile ID $billingProfileId", cause)
    )

object BillingProfileManagerDAO {
  val BillingProfileRequestBatchSize = 1000

  object ProfilePolicy extends Enumeration {
    type ProfilePolicy = Value
    val Owner: ProfilePolicy = Value("owner")
    val User: ProfilePolicy = Value("user")
    val PetCreator: ProfilePolicy = Value("pet-creator")

    def fromProjectRole(projectRole: ProjectRole): ProfilePolicy =
      projectRole match {
        case ProjectRoles.Owner => Owner
        case ProjectRoles.User  => User
      }
  }
}

/**
 * Facade over the billing profile manager service. This service will eventually by the source of truth for
 * billing profiles in the Terra system. For now, we are using this to layer in "external" billing profiles
 * for the purposes of testing Azure workspaces.
 */
class BillingProfileManagerDAOImpl(
  apiClientProvider: BillingProfileManagerClientProvider,
  config: MultiCloudWorkspaceConfig
) extends BillingProfileManagerDAO
    with LazyLogging {

  override def listManagedApps(subscriptionId: UUID,
                               includeAssignedApps: Boolean,
                               ctx: RawlsRequestContext
  ): Seq[AzureManagedAppModel] = {
    val azureApi = apiClientProvider.getAzureApi(ctx)

    azureApi.getManagedAppDeployments(subscriptionId, includeAssignedApps).getManagedApps.asScala.toList
  }

  override def createBillingProfile(
    displayName: String,
    billingInfo: Either[RawlsBillingAccountName, AzureManagedAppCoordinates],
    policies: Map[String, List[(String, String)]] = Map.empty,
    ctx: RawlsRequestContext
  ): ProfileModel = {

    val policyInputs = new BpmApiPolicyInputs().inputs(
      policies
        .map { case (policyName, additionalData) =>
          new BpmApiPolicyInput()
            .namespace("terra") // policy namespaces in Rawls are always 'terra'
            .name(policyName)
            .additionalData(
              additionalData.map { case (key, value) =>
                new BpmApiPolicyPair().key(key).value(value)
              }.asJava
            )
        }
        .toList
        .asJava
    )

    // create the profile
    val profileApi = apiClientProvider.getProfileApi(ctx)
    val createProfileRequest = billingInfo match {
      case Left(billingAccountName) =>
        val rawlsBillingAccountName = billingAccountName
        new CreateProfileRequest()
          .billingAccountId(rawlsBillingAccountName.withoutPrefix())
          .displayName(displayName)
          .id(UUID.randomUUID())
          .biller("direct") // community terra is always 'direct' (i.e., no reseller)
          .cloudPlatform(CloudPlatform.GCP)
          .policies(policyInputs)

      case Right(coords) =>
        val azureManagedAppCoordinates = coords
        new CreateProfileRequest()
          .tenantId(azureManagedAppCoordinates.tenantId)
          .subscriptionId(azureManagedAppCoordinates.subscriptionId)
          .managedResourceGroupId(azureManagedAppCoordinates.managedResourceGroupId)
          .displayName(displayName)
          .id(UUID.randomUUID())
          .biller("direct") // community terra is always 'direct' (i.e., no reseller)
          .cloudPlatform(CloudPlatform.AZURE)
          .policies(policyInputs)
    }

    logger.info(s"Creating billing profile [id=${createProfileRequest.getId}]")
    profileApi.createProfile(createProfileRequest)
  }

  def getBillingProfile(billingProfileId: UUID, ctx: RawlsRequestContext): Option[ProfileModel] =
    Try(Option(apiClientProvider.getProfileApi(ctx).getProfile(billingProfileId))) match {
      case Success(value) => value
      case Failure(e: ApiException)
          if e.getCode == StatusCodes.NotFound.intValue || e.getCode == StatusCodes.Forbidden.intValue =>
        None
      case Failure(e) => throw new BpmException(billingProfileId, e);
    }

  override def deleteBillingProfile(billingProfileId: UUID, ctx: RawlsRequestContext): Unit =
    Try(apiClientProvider.getProfileApi(ctx).deleteProfile(billingProfileId)) match {
      case Failure(exception: ApiException) if exception.getCode == HttpStatus.SC_NOT_FOUND =>
        logger.info(s"No billing profile record found for $billingProfileId when deleting BPM-backed billing project")
      case Failure(t) => throw t
      case Success(_) => ()
    }

  def getAllBillingProfiles(ctx: RawlsRequestContext)(implicit ec: ExecutionContext): Future[Seq[ProfileModel]] = {
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

    Future.successful(callListProfiles())
  }

  def addProfilePolicyMember(billingProfileId: UUID,
                             policy: ProfilePolicy,
                             memberEmail: String,
                             ctx: RawlsRequestContext
  ): Unit =
    apiClientProvider
      .getProfileApi(ctx)
      .addProfilePolicyMember(
        new PolicyMemberRequest().email(memberEmail),
        billingProfileId,
        policy.toString
      )

  def deleteProfilePolicyMember(billingProfileId: UUID,
                                policy: ProfilePolicy,
                                memberEmail: String,
                                ctx: RawlsRequestContext
  ): Unit =
    apiClientProvider
      .getProfileApi(ctx)
      .deleteProfilePolicyMember(
        billingProfileId,
        policy.toString,
        memberEmail
      )

  override def getStatus(): SystemStatus = apiClientProvider.getUnauthenticatedApi().serviceStatus()

  @throws(classOf[BpmAzureSpendReportApiException])
  def getAzureSpendReport(billingProfileId: UUID,
                          spendReportStartDate: Date,
                          spendReportEndDate: Date,
                          ctx: RawlsRequestContext
  ): SpendReport =
    try
      apiClientProvider
        .getSpendReportingApi(ctx)
        .getSpendReport(
          billingProfileId,
          spendReportStartDate,
          spendReportEndDate
        )
    catch {
      case ex: ApiException =>
        logger.info(s"Failed to get Azure spend report for billing profile [id=${billingProfileId}]")
        val bpmErrorMessageJson = ex.getMessage.parseJson
        val bpmErrorMessage = bpmErrorMessageJson.convertTo[BpmAzureReportErrorMessage]
        throw new BpmAzureSpendReportApiException(ex.getCode, bpmErrorMessage.message, ex)
    }
}
