package org.broadinstitute.dsde.rawls.webservice

import com.google.api.services.storage.model.Bucket
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.{MockGoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.StatusJsonSupport.StatusCheckResponseFormat
import org.broadinstitute.dsde.rawls.model.Subsystems._
import org.broadinstitute.dsde.rawls.model.{StatusCheckResponse, SubsystemStatus}
import org.broadinstitute.dsde.rawls.monitor.HealthMonitor
import org.broadinstitute.dsde.rawls.monitor.HealthMonitor.CheckAll
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route.{seal => sealRoute}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by rtitle on 5/21/17.
  */


class MockGoogleServicesErrorDAO extends MockGoogleServicesDAO("test") {
  override def getBucket(bucketName: String)(implicit executionContext: ExecutionContext): Future[Option[Bucket]] = Future.successful(None)
}

class StatusApiServiceSpec extends ApiServiceSpec with Eventually  {
  // This configures how long the calls to `whenReady(Future)` and `eventually` will wait
  // before giving up and failing the test.
  // See: http://doc.scalatest.org/2.2.4/index.html#org.scalatest.concurrent.Futures
  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)))

  case class TestApiService(dataSource: SlickDataSource, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(implicit override val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectives

  def withApiServices[T](dataSource: SlickDataSource, subsystemsOk: Boolean, apiService: TestApiService)(testCode: TestApiService => T): T = {
    try {
      initializeSubsystems(apiService, subsystemsOk)
      testCode(apiService)
    } finally {
      apiService.cleanupSupervisor
    }
  }

  def withConstantTestDataApiServices[T](subsystemsOk: Boolean)(testCode: TestApiService => T): T = {
    withConstantTestDatabase { dataSource: SlickDataSource =>
      val apiService = new TestApiService(dataSource, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO)
      withApiServices(dataSource, subsystemsOk, apiService)(testCode)
    }
  }

  def withConstantErrorTestDataApiServices[T](subsystemsOk: Boolean)(testCode: TestApiService => T): T = {
    withConstantTestDatabase { dataSource: SlickDataSource =>
      val apiService = new TestApiService(dataSource, new MockGoogleServicesErrorDAO, new MockGooglePubSubDAO)
      withApiServices(dataSource, subsystemsOk, apiService)(testCode)
    }
  }

  def initializeSubsystems(apiService: TestApiService, subsystemsOk: Boolean) = {
    apiService.healthMonitor ! CheckAll
  }

  "StatusApiService" should "return 200 for ok status" in withConstantTestDataApiServices(true) { services =>
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

  it should "return 500 for non-ok status for any subsystem" in withConstantErrorTestDataApiServices(false) { services =>
    eventually {
      withStatsD {
        Get("/status") ~>
          services.sealedInstrumentedRoutes ~>
          check {
            assertResult(StatusCodes.InternalServerError) {
              status
            }
            assertResult(StatusCheckResponse(false, AllSubsystems.map {
              case GoogleBuckets => GoogleBuckets -> SubsystemStatus(false, Some(List("Could not find bucket: my-favorite-bucket")))
              case other => other -> HealthMonitor.OkStatus
            }.toMap)) {
              responseAs[StatusCheckResponse]
            }
          }
      } { capturedMetrics =>
        val expected = expectedHttpRequestMetrics("get", "status", StatusCodes.InternalServerError.intValue, 1)
        assertSubsetOf(expected, capturedMetrics)
      }
    }
  }

  List(CONNECT, DELETE, HEAD, OPTIONS, PATCH, POST, PUT, TRACE) foreach { method =>
    it should s"return 405 for $method requests" in withConstantTestDataApiServices(true) { services =>
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
