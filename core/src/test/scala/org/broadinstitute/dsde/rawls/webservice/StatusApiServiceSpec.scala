package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import bio.terra.profile.model.SystemStatus
import com.google.api.services.storage.model.Bucket
import org.broadinstitute.dsde.rawls.dataaccess.{MockGoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.StatusJsonSupport.StatusCheckResponseFormat
import org.broadinstitute.dsde.rawls.model.Subsystems._
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, RawlsBillingAccount, StatusCheckResponse, SubsystemStatus}
import org.broadinstitute.dsde.rawls.monitor.HealthMonitor
import org.broadinstitute.dsde.rawls.monitor.HealthMonitor.CheckAll
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.mockito.Mockito.when
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.{ExecutionContext, Future}

class MockGoogleServicesErrorDAO extends MockGoogleServicesDAO("test") {
  override def getBucket(bucketName: String, userProject: Option[GoogleProjectId])(implicit
    executionContext: ExecutionContext
  ): Future[Either[String, Bucket]] = Future.successful(Left("No bucket in this mock"))
}

class MockGoogleServicesCriticalErrorDAO extends MockGoogleServicesDAO("test") {
  override def listBillingAccountsUsingServiceCredential(implicit
    executionContext: ExecutionContext
  ): Future[Seq[RawlsBillingAccount]] =
    Future.successful(Seq.empty)
}

class StatusApiServiceSpec extends ApiServiceSpec with Eventually {
  // This configures how long the calls to `whenReady(Future)` and `eventually` will wait
  // before giving up and failing the test.
  // See: http://doc.scalatest.org/2.2.4/index.html#org.scalatest.concurrent.Futures
  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)))

  case class TestApiService(dataSource: SlickDataSource, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(
    implicit override val executionContext: ExecutionContext
  ) extends ApiServices
      with MockUserInfoDirectives

  def withApiServices[T](dataSource: SlickDataSource, apiService: TestApiService)(
    testCode: TestApiService => Any
  ): Any =
    try {
      initializeSubsystems(apiService)
      testCode(apiService)
    } finally
      apiService.cleanupSupervisor

  def withConstantTestDataApiServices[T](testCode: TestApiService => Any): Any =
    withConstantTestDatabase { dataSource: SlickDataSource =>
      val apiService = new TestApiService(dataSource, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO)
      withApiServices(dataSource, apiService)(testCode)
    }

  def withConstantCriticalErrorTestDataApiServices[T](testCode: TestApiService => Any): Any =
    withConstantTestDatabase { dataSource: SlickDataSource =>
      val apiService = new TestApiService(dataSource, new MockGoogleServicesCriticalErrorDAO, new MockGooglePubSubDAO)
      withApiServices(dataSource, apiService)(testCode)
    }

  def withConstantErrorTestDataApiServices[T](testCode: TestApiService => Any): Any =
    withConstantTestDatabase { dataSource: SlickDataSource =>
      val apiService = new TestApiService(dataSource, new MockGoogleServicesErrorDAO, new MockGooglePubSubDAO)
      withApiServices(dataSource, apiService)(testCode)
    }

  def initializeSubsystems(apiService: TestApiService): Unit = {
    when(apiService.billingProfileManagerDAO.getStatus()) thenReturn new SystemStatus().ok(true)
    apiService.healthMonitor ! CheckAll
  }

  "StatusApiService" should "return 200 for ok status" in withConstantTestDataApiServices { services =>
    eventually {
      withStatsD {
        Get("/status") ~>
          services.sealedInstrumentedRoutes ~>
          check {
            assertResult(StatusCodes.OK) {
              status
            }
            assertResult(StatusCheckResponse(true, AllSubsystems.map(_ -> HealthMonitor.OkStatus).toMap)) {
              responseAs[StatusCheckResponse]
            }
          }
      } { capturedMetrics =>
        val expected = expectedHttpRequestMetrics("get", "status", StatusCodes.OK.intValue, 1)
        assertSubsetOf(expected, capturedMetrics)
      }
    }
  }

  it should "return 500 for non-ok status for critical subsystem" in withConstantCriticalErrorTestDataApiServices {
    services =>
      eventually {
        withStatsD {
          Get("/status") ~>
            services.sealedInstrumentedRoutes ~>
            check {
              assertResult(StatusCodes.InternalServerError, responseAs[StatusCheckResponse]) {
                status
              }
              assertResult(
                StatusCheckResponse(
                  false,
                  AllSubsystems.map {
                    case GoogleGroups =>
                      GoogleGroups -> SubsystemStatus(false, Some(List("Could not find group: my-favorite-group")))
                    case other => other -> HealthMonitor.OkStatus
                  }.toMap
                )
              ) {
                responseAs[StatusCheckResponse]
              }
            }
        } { capturedMetrics =>
          val expected = expectedHttpRequestMetrics("get", "status", StatusCodes.InternalServerError.intValue, 1)
          assertSubsetOf(expected, capturedMetrics)
        }
      }
  }

  it should "return 200 for non-ok status for any non critical subsystem" in withConstantErrorTestDataApiServices {
    services =>
      eventually {
        withStatsD {
          Get("/status") ~>
            services.sealedInstrumentedRoutes ~>
            check {
              assertResult(StatusCodes.OK) {
                status
              }
              assertResult(
                StatusCheckResponse(
                  false,
                  AllSubsystems.map {
                    case GoogleBuckets =>
                      GoogleBuckets -> SubsystemStatus(
                        false,
                        Some(List("Could not find bucket: my-favorite-bucket. No bucket in this mock"))
                      )
                    case other => other -> HealthMonitor.OkStatus
                  }.toMap
                )
              ) {
                responseAs[StatusCheckResponse]
              }
            }
        } { capturedMetrics =>
          val expected = expectedHttpRequestMetrics("get", "status", StatusCodes.OK.intValue, 1)
          assertSubsetOf(expected, capturedMetrics)
        }
      }
  }

  List(CONNECT, DELETE, HEAD, OPTIONS, PATCH, POST, PUT, TRACE) foreach { method =>
    it should s"return 405 for $method requests" in withConstantTestDataApiServices { services =>
      new RequestBuilder(method).apply("/status") ~>
        sealRoute(services.statusRoute) ~>
        check {
          assertResult(StatusCodes.MethodNotAllowed) {
            status
          }
        }
    }
  }

}
