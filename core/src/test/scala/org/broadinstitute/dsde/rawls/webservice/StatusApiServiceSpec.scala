package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.dataaccess.{MockGoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.{RawlsUserSubjectId, StatusCheckResponse, SubsystemStatus}
import org.broadinstitute.dsde.rawls.model.Subsystems._
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.StatusCheckResponseFormat
import org.broadinstitute.dsde.rawls.monitor.HealthMonitor
import org.broadinstitute.dsde.rawls.monitor.HealthMonitor.CheckAll
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}
import spray.http.StatusCodes
import spray.http.HttpMethods._

import scala.concurrent.ExecutionContext

/**
  * Created by rtitle on 5/21/17.
  */
class StatusApiServiceSpec extends ApiServiceSpec with Eventually  {
  // This configures how long the calls to `whenReady(Future)` and `eventually` will wait
  // before giving up and failing the test.
  // See: http://doc.scalatest.org/2.2.4/index.html#org.scalatest.concurrent.Futures
  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)))

  case class TestApiService(dataSource: SlickDataSource, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(implicit val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectives

  def withApiServices[T](dataSource: SlickDataSource)(testCode: TestApiService => T): T = {
    val apiService = new TestApiService(dataSource, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO)
    try {
      initializeSubsystems(apiService)
      testCode(apiService)
    } finally {
      apiService.cleanupSupervisor
    }
  }

  def withConstantTestDataApiServices[T](testCode: TestApiService => T): T = {
    withConstantTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }
  }

  def initializeSubsystems(apiService: TestApiService) = {
    apiService.directoryDAO.createUser(testData.userOwner.userSubjectId)
    apiService.healthMonitor ! CheckAll
  }

  "StatusApiService" should "return ok status" in withConstantTestDataApiServices { services =>
    eventually {
      Get("/status") ~>
        sealRoute(services.publicStatusRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(StatusCheckResponse(true, AllSubsystems.map(_ -> SubsystemStatus(true, Seq.empty)).toMap)) {
            responseAs[StatusCheckResponse]
          }
        }
    }
  }

  List(CONNECT, DELETE, HEAD, OPTIONS, PATCH, POST, PUT, TRACE) foreach { method =>
    it should s"return 405 for $method requests" in withConstantTestDataApiServices { services =>
      new RequestBuilder(method).apply("/status") ~>
        sealRoute(services.publicStatusRoutes) ~>
        check {
          assertResult(StatusCodes.MethodNotAllowed) {
            status
          }
        }
    }
  }

}
