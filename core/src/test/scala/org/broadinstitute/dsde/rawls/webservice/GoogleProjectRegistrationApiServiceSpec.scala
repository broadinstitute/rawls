package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Directive1
import akka.http.scaladsl.server.Directives.provide
import io.opentelemetry.context.Context
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.googleProject.{
  GoogleProjectRegistrationRepository,
  GoogleProjectRegistrationService
}
import org.broadinstitute.dsde.rawls.model.GoogleProjectRegistrationJsonSupport$._
import org.broadinstitute.dsde.rawls.model._
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.Future

class GoogleProjectRegistrationApiServiceSpec
    extends ApiServiceSpec
    with MockitoSugar
    with GoogleProjectRegistrationApiService {

  val mockGoogleProjectRegService: GoogleProjectRegistrationService = mock[GoogleProjectRegistrationService]
  val mockSamDAO: SamDAO = mock[SamDAO]
  val mockGoogleProjectRegRepo: GoogleProjectRegistrationRepository = mock[GoogleProjectRegistrationRepository]

  override val googleProjectRegServiceConstructor: RawlsRequestContext => GoogleProjectRegistrationService = { _ =>
    mockGoogleProjectRegService
  }

  override def requireUserInfo(otelContext: Option[Context]): Directive1[UserInfo] =
    provide(
      UserInfo(RawlsUserEmail("test-email"), OAuth2BearerToken("token"), 3600, RawlsUserSubjectId("test-user-id"))
    )

  "GoogleProjectRegistrationApiService" should "retrieve Google projects filtered by billing project name" in {
    val googleProjectRegistration = GoogleProjectRegistration(
      GoogleProjectId("test-project-id"),
      Some(RawlsBillingAccountName("billing-account")),
      None,
      RawlsBillingProjectName("test-billing-project")
    )

    when(
      mockGoogleProjectRegService.getGoogleProjects(Some(RawlsBillingProjectName("test-billing-project")), 10, 0)
    )
      .thenReturn(Future.successful(Seq(googleProjectRegistration)))

    Get(
      "/googleProjects?billingProjectId=test-billing-project&pageSize=10&offset=0"
    ) ~> googleProjectRegistrationRoutes() ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[Seq[GoogleProjectRegistration]] shouldEqual Seq(googleProjectRegistration)
    }
  }

  it should "return NotFound if billing project name is set but no matches are found" in {
    when(
      mockGoogleProjectRegService.getGoogleProjects(Some(RawlsBillingProjectName("non-existent-billing-project")),
                                                    10,
                                                    0
      )
    )
      .thenReturn(Future.successful(Seq.empty))

    Get(
      "/googleProjects?billingProjectId=non-existent-billing-project&pageSize=10&offset=0"
    ) ~> googleProjectRegistrationRoutes() ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[Seq[GoogleProjectRegistration]] shouldEqual Seq.empty
    }
  }

  it should "retrieve all Google projects if no billing project name is provided" in {
    val googleProjectRegistration1 = GoogleProjectRegistration(
      GoogleProjectId("test-project-id-1"),
      Some(RawlsBillingAccountName("billing-account-1")),
      None,
      RawlsBillingProjectName("test-billing-project-1")
    )

    val googleProjectRegistration2 = GoogleProjectRegistration(
      GoogleProjectId("test-project-id-2"),
      Some(RawlsBillingAccountName("billing-account-2")),
      None,
      RawlsBillingProjectName("test-billing-project-2")
    )

    when(mockGoogleProjectRegService.getGoogleProjects(None, 10, 0))
      .thenReturn(Future.successful(Seq(googleProjectRegistration1, googleProjectRegistration2)))

    Get("/googleProjects?pageSize=10&offset=0") ~> googleProjectRegistrationRoutes() ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[Seq[GoogleProjectRegistration]] shouldEqual Seq(googleProjectRegistration1, googleProjectRegistration2)
    }
  }

  it should "retrieve a Google project by ID" in {
    val googleProjectRegistration = GoogleProjectRegistration(
      GoogleProjectId("test-project-id"),
      Some(RawlsBillingAccountName("billing-account")),
      None,
      RawlsBillingProjectName("test-billing-project")
    )

    when(mockGoogleProjectRegService.getGoogleProjectById(GoogleProjectId("test-project-id")))
      .thenReturn(Future.successful(Some(googleProjectRegistration)))

    Get("/googleProjects/test-project-id") ~> googleProjectRegistrationRoutes() ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[GoogleProjectRegistration] shouldEqual googleProjectRegistration
    }
  }
}
