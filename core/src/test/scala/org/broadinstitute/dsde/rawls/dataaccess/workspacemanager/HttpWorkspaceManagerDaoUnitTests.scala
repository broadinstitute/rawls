package org.broadinstitute.dsde.rawls.dataaccess.workspacemanager

import akka.actor.ActorSystem
import bio.terra.workspace.api.LandingZonesApi
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model.DeleteAzureLandingZoneRequestBody
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.model.RawlsRequestContext
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{verify, when}
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID

class HttpWorkspaceManagerDaoUnitTests extends AnyFlatSpec with OptionValues with MockitoSugar with MockitoTestUtils with Matchers {

  implicit val actorSystem: ActorSystem = ActorSystem("HttpWorkspaceManagerDAOSpec")
  implicit val executionContext: TestExecutionContext = TestExecutionContext.testExecutionContext

  behavior of "deleteLandingZone"

  it should "return None a 404 is returned when deleting a landing zone" in {

    val landingZonesApi = mock[LandingZonesApi]
    when(landingZonesApi.deleteAzureLandingZone(any[DeleteAzureLandingZoneRequestBody], any[UUID]))
      .thenThrow(new ApiException(404, "Not found"))

    val apiClientProvider = mock[WorkspaceManagerApiClientProvider]
    when(apiClientProvider.getLandingZonesApi(any)).thenReturn(landingZonesApi)
    val wsmDao =
      new HttpWorkspaceManagerDAO(apiClientProvider)
    val landingZoneId = UUID.randomUUID()

    val testContext = mock[RawlsRequestContext]

    wsmDao.deleteLandingZone(landingZoneId, testContext) shouldBe None

    verify(landingZonesApi).deleteAzureLandingZone(any[DeleteAzureLandingZoneRequestBody],
      ArgumentMatchers.eq(landingZoneId)
    )

  }

  it should "return None if a 403 is returned to indicate the landing zone is not present when deleting" in {

    val landingZonesApi = mock[LandingZonesApi]
    when(landingZonesApi.deleteAzureLandingZone(any[DeleteAzureLandingZoneRequestBody], any[UUID]))
      .thenThrow(new ApiException(403, "Forbidden"))

    val apiClientProvider = mock[WorkspaceManagerApiClientProvider]
    when(apiClientProvider.getLandingZonesApi(any)).thenReturn(landingZonesApi)
    val wsmDao =
      new HttpWorkspaceManagerDAO(apiClientProvider)
    val landingZoneId = UUID.randomUUID()

    val testContext = mock[RawlsRequestContext]

    wsmDao.deleteLandingZone(landingZoneId, testContext) shouldBe None

    verify(landingZonesApi).deleteAzureLandingZone(any[DeleteAzureLandingZoneRequestBody],
      ArgumentMatchers.eq(landingZoneId)
    )

  }


  it should "rethrow an API exception that is not a 404 or 403" in {

    val landingZonesApi = mock[LandingZonesApi]
    when(landingZonesApi.deleteAzureLandingZone(any[DeleteAzureLandingZoneRequestBody], any[UUID]))
      .thenThrow(new ApiException(500, "error"))

    val apiClientProvider = mock[WorkspaceManagerApiClientProvider]
    when(apiClientProvider.getLandingZonesApi(any)).thenReturn(landingZonesApi)
    val wsmDao =
      new HttpWorkspaceManagerDAO(apiClientProvider)
    val landingZoneId = UUID.randomUUID()

    val testContext = mock[RawlsRequestContext]

    intercept[ApiException] { wsmDao.deleteLandingZone(landingZoneId, testContext) } shouldBe a[ApiException]

    verify(landingZonesApi).deleteAzureLandingZone(any[DeleteAzureLandingZoneRequestBody],
      ArgumentMatchers.eq(landingZoneId)
    )

  }

}
