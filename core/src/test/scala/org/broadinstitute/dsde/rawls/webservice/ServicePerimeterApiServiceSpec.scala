package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{
  RawlsBillingProjectOperationRecord,
  RawlsBillingProjectRecord,
  ReadAction
}
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.broadinstitute.dsde.rawls.{model, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.mockito.{ArgumentMatcher, ArgumentMatchers}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import spray.json.DefaultJsonProtocol._

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.{ExecutionContext, Future}

class ServicePerimeterApiServiceSpec extends ApiServiceSpec with MockitoSugar {
  case class TestApiService(dataSource: SlickDataSource, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(
    implicit override val executionContext: ExecutionContext
  ) extends ApiServices
      with MockUserInfoDirectives {
    override val samDAO: SamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    when(
      samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
                           any[String],
                           any[SamResourceAction],
                           any[RawlsRequestContext]
      )
    ).thenReturn(Future.successful(true))
    when(
      samDAO.addUserToPolicy(ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
                             any[String],
                             any[SamResourcePolicyName],
                             any[String],
                             any[RawlsRequestContext]
      )
    ).thenReturn(Future.successful(()))
    when(
      samDAO.removeUserFromPolicy(ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
                                  any[String],
                                  any[SamResourcePolicyName],
                                  any[String],
                                  any[RawlsRequestContext]
      )
    ).thenReturn(Future.successful(()))
  }

  def withApiServices[T](dataSource: SlickDataSource,
                         gcsDAO: MockGoogleServicesDAO = new MockGoogleServicesDAO("test")
  )(testCode: TestApiService => T): T = {
    val apiService = TestApiService(dataSource, gcsDAO, new MockGooglePubSubDAO)
    try
      testCode(apiService)
    finally
      apiService.cleanupSupervisor
  }

  def withTestDataApiServices[T](testCode: TestApiService => T): T =
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }

  it should "return 204 when adding a billing project to a service perimeter with all the right permissions" in withTestDataApiServices {
    services =>
      val projectName = testData.billingProject.projectName
      val servicePerimeterName = ServicePerimeterName("accessPolicies/123/servicePerimeters/service_perimeter")
      val encodedServicePerimeterName = URLEncoder.encode(servicePerimeterName.value, UTF_8.name)

      when(
        services.samDAO.userHasAction(
          ArgumentMatchers.eq(SamResourceTypeNames.servicePerimeter),
          ArgumentMatchers.eq(encodedServicePerimeterName),
          ArgumentMatchers.eq(SamServicePerimeterActions.addProject),
          ArgumentMatchers.argThat(userInfoEq(testContext))
        )
      ).thenReturn(Future.successful(true))

      Put(s"/servicePerimeters/${encodedServicePerimeterName}/projects/${projectName.value}") ~>
        sealRoute(services.servicePerimeterRoutes) ~>
        check {
          assertResult(StatusCodes.NoContent) {
            status
          }
        }
  }

  it should "return 403 when adding a billing project to a service perimeter without the right permission on the billing project" in withTestDataApiServices {
    services =>
      val projectName = testData.billingProject.projectName
      val servicePerimeterName = ServicePerimeterName("accessPolicies/123/servicePerimeters/service_perimeter")
      val encodedServicePerimeterName = URLEncoder.encode(servicePerimeterName.value, UTF_8.name)

      when(
        services.samDAO.userHasAction(
          ArgumentMatchers.eq(SamResourceTypeNames.servicePerimeter),
          ArgumentMatchers.eq(encodedServicePerimeterName),
          ArgumentMatchers.eq(SamServicePerimeterActions.addProject),
          ArgumentMatchers.argThat(userInfoEq(testContext))
        )
      ).thenReturn(Future.successful(true))
      when(
        services.samDAO.userHasAction(
          ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
          ArgumentMatchers.eq(projectName.value),
          ArgumentMatchers.eq(SamBillingProjectActions.addToServicePerimeter),
          ArgumentMatchers.argThat(userInfoEq(testContext))
        )
      ).thenReturn(Future.successful(false))

      Put(s"/servicePerimeters/${encodedServicePerimeterName}/projects/${projectName.value}") ~>
        sealRoute(services.servicePerimeterRoutes) ~>
        check {
          assertResult(StatusCodes.Forbidden) {
            status
          }
        }
  }

  it should "return 404 when adding a billing project to a service perimeter without the right permission on the service perimeter" in withTestDataApiServices {
    services =>
      val projectName = testData.billingProject.projectName
      val servicePerimeterName = ServicePerimeterName("accessPolicies/123/servicePerimeters/service_perimeter")
      val encodedServicePerimeterName = URLEncoder.encode(servicePerimeterName.value, UTF_8.name)

      when(
        services.samDAO.userHasAction(
          ArgumentMatchers.eq(SamResourceTypeNames.servicePerimeter),
          ArgumentMatchers.eq(encodedServicePerimeterName),
          ArgumentMatchers.eq(SamServicePerimeterActions.addProject),
          ArgumentMatchers.argThat(userInfoEq(testContext))
        )
      ).thenReturn(Future.successful(false))

      Put(s"/servicePerimeters/${encodedServicePerimeterName}/projects/${projectName.value}") ~>
        sealRoute(services.servicePerimeterRoutes) ~>
        check {
          assertResult(StatusCodes.NotFound) {
            status
          }
        }
  }
}
