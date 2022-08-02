package org.broadinstitute.dsde.rawls.billing
import akka.http.scaladsl.model.StatusCodes
import bio.terra.profile.api.{AzureApi, ProfileApi}
import bio.terra.profile.client.ApiClient
import bio.terra.profile.model.{AzureManagedAppModel, AzureManagedAppsResponseModel, CloudPlatform, CreateProfileRequest, ProfileModel}
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.model.{AzureManagedAppCoordinates, CreateRawlsV2BillingProjectFullRequest, ErrorReport, UserInfo}

import scala.jdk.CollectionConverters._
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class AzureBillingProjectCreator(billingRepository: BillingRepository, billingProfileManagerDAO: BillingProfileManagerDAO) extends BillingProjectCreator {
  override def validateBillingProjectCreationRequest(createProjectRequest: CreateRawlsV2BillingProjectFullRequest, userInfo: UserInfo)(implicit executionContext: ExecutionContext): Future[Unit] = {
    Future.successful()
  }

  override def postCreationSteps(createProjectRequest: CreateRawlsV2BillingProjectFullRequest, userInfo: UserInfo)(implicit executionContext: ExecutionContext): Future[Unit] = {

    val coords: AzureManagedAppCoordinates = createProjectRequest.managedAppCoordinates match {
      case None => throw new InvalidCreationRequest(ErrorReport(StatusCodes.BadRequest, "Invalid request"))
      case Some(foo) => foo
    }
    for {
      profileModel <- billingProfileManagerDAO.createBillingProfile(createProjectRequest.projectName.value, Right(coords), userInfo)
      _ <- billingRepository.setBillingProfileId(createProjectRequest.projectName, profileModel.getId)
    } yield {}
  }

  private def getApiClient() = {
    val client: ApiClient = new ApiClient()
    client.setBasePath("foo")
    client.setAccessToken("foo")

    client
  }

  private def getAzureApi() = {
    new AzureApi(getApiClient())
  }

  private def getProfileApi() = {
    new ProfileApi(getApiClient())
  }
}


class ManagedApplicationAccessException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)
class InvalidCreationRequest(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)
