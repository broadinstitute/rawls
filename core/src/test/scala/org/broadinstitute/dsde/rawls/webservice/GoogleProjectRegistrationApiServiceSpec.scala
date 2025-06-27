package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Directive1
import akka.http.scaladsl.server.Directives.provide
import io.opentelemetry.context.Context
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.googleProject.{
  GoogleProjectRegistrationRepository,
  GoogleProjectRegistrationService
}
import org.broadinstitute.dsde.rawls.model.GoogleProjectRegistrationJsonSupport$._
import org.broadinstitute.dsde.rawls.model._
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

    when(mockGoogleProjectRegService.getGoogleProjects(Some(RawlsBillingProjectName("test-billing-project")), 10, 0))
      .thenReturn(Future.successful(Seq(googleProjectRegistration)))

    Get(
      "/googleProjects?billingProjectId=test-billing-project&pageSize=10&offset=0"
    ) ~> googleProjectRegistrationRoutes(userInfo = userInfo) ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[Seq[GoogleProjectRegistration]] shouldEqual Seq(googleProjectRegistration)
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

    Get("/googleProjects/test-project-id") ~> googleProjectRegistrationRoutes(userInfo = userInfo) ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[GoogleProjectRegistration] shouldEqual googleProjectRegistration
    }
  }

  it should "return 404 Not Found if the Google project is not found" in {
    when(mockGoogleProjectRegService.getGoogleProjectById(GoogleProjectId("non-existent-project-id")))
      .thenReturn(Future.successful(None))

    Get("/googleProjects/non-existent-project-id") ~> googleProjectRegistrationRoutes(userInfo = userInfo) ~> check {
      status shouldEqual StatusCodes.NotFound
      responseAs[String] shouldEqual "Google project does not exist or you don't have access."
    }
  }
}
