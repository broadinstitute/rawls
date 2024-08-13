package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import akka.http.scaladsl.server.Directives._
import io.sentry.Sentry
import io.sentry.protocol.SentryId
import org.broadinstitute.dsde.rawls.model.ErrorReport
import org.mockito.ArgumentMatchers.any
import spray.json.RootJsonFormat
import org.mockito.MockedStatic
import org.mockito.Mockito.mockStatic

import java.sql.SQLException
import java.util.UUID
import org.broadinstitute.dsde.rawls.dataaccess.{HttpExecutionServiceDAO, MockShardedExecutionServiceCluster}
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.ExecutionServiceVersionFormat
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport.ApplicationVersionFormat
import org.broadinstitute.dsde.rawls.model.{ApplicationVersion, ExecutionServiceVersion}
import spray.json.DefaultJsonProtocol._

import scala.language.postfixOps
import java.sql.SQLTransactionRollbackException
import scala.concurrent.ExecutionContext
import scala.util.Using

/**
 * Created by dvoet on 1/26/16.
 */
class RawlsApiServiceSpec extends ApiServiceSpec with VersionApiService {
  override val executionServiceCluster = MockShardedExecutionServiceCluster.fromDAO(
    new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, workbenchMetricBaseName = workbenchMetricBaseName),
    slickDataSource
  )
  override val appVersion = ApplicationVersion("githash", "buildnumber", "version")

  "RootRawlsApiService" should "get a version" in {
    withStatsD {
      Get("/version") ~>
        sealRoute(captureRequestMetrics(traceRequests(_ => versionRoutes))) ~>
        check {
          assertResult(StatusCodes.OK)(status)
          assertResult(appVersion)(responseAs[ApplicationVersion])
        }
    } { capturedMetrics =>
      val expected = expectedHttpRequestMetrics("get", "version", StatusCodes.OK.intValue, 1)
      assertSubsetOf(expected, capturedMetrics)
    }
  }

  it should "return the cromwell version" in {
    Get("/version/executionEngine") ~>
      sealRoute(versionRoutes) ~>
      check {
        assertResult(StatusCodes.OK)(status)
        responseAs[ExecutionServiceVersion]
      }
  }

  behavior of "ExceptionHandler"

  import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._

  def testRoute(callback: () => Unit = () => ()): server.Route = {
    implicit val ec: ExecutionContext = ExecutionContext.global
    case class TestResponse(ok: Boolean)
    implicit val TestResponseFormat: RootJsonFormat[TestResponse] = jsonFormat1(TestResponse)
    path("test") {
      get {
        complete {
          callback()
          StatusCodes.OK -> TestResponse(true)
        }
      }
    }
  }

  it should "does not trigger for normal completion" in {
    val routes = sealRoute(handleExceptions(RawlsApiService.exceptionHandler) {
      testRoute()
    })
    Get("/test") ~>
      routes ~>
      check {
        status shouldBe StatusCodes.OK
      }
  }

  it should "wrap a SQLTransactionRollbackException in an ErrorReport" in {
    val sqlException = new SQLTransactionRollbackException("some message")
    val routes = sealRoute(handleExceptions(RawlsApiService.exceptionHandler) {
      testRoute(() => throw sqlException)
    })
    Get("/test") ~>
      routes ~>
      check {
        status shouldBe StatusCodes.InternalServerError
        val report = responseAs[ErrorReport]
        report.message should include("some message")
      }
  }

  it should "expose a generic error message with an errorId for an SQLException" in
    Using(mockStatic(classOf[Sentry])) { sentry =>
      val errorId = UUID.randomUUID()
      // we need to explicitly de-sugar the mocking setup,
      // because the scala compiler doesn't do the same implicit casting as java
      sentry
        .when(new MockedStatic.Verification {
          override def apply(): Unit = Sentry.captureException(any[Throwable])
        })
        .thenReturn(new SentryId(errorId))
      val sqlException = new SQLException("some message")
      val routes = sealRoute(handleExceptions(RawlsApiService.exceptionHandler) {
        testRoute(() => throw sqlException)
      })
      Get("/test") ~>
        routes ~>
        check {
          status shouldBe StatusCodes.InternalServerError
          val report = responseAs[ErrorReport]
          report.message should not include "some message"
          report.message should include(errorId.toString.replace("-", ""))
        }
    }

}
