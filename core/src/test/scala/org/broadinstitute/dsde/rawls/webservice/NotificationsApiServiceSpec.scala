package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.dataaccess.{MockGoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import spray.json.DefaultJsonProtocol._

import scala.concurrent.ExecutionContext

class NotificationsApiServiceSpec extends ApiServiceSpec {
  case class TestApiService(dataSource: SlickDataSource, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(
    implicit val executionContext: ExecutionContext
  ) extends ApiServices
      with MockUserInfoDirectives

  def withApiServices[T](dataSource: SlickDataSource,
                         gcsDAO: MockGoogleServicesDAO = new MockGoogleServicesDAO("test")
  )(testCode: TestApiService => T): T = {
    val apiService = new TestApiService(dataSource, gcsDAO, new MockGooglePubSubDAO)
    try
      testCode(apiService)
    finally
      apiService.cleanupSupervisor
  }

  "NotificationsApiService" should "get workspace notifications" in withEmptyTestDatabase {
    slickDataSource: SlickDataSource =>
      withApiServices(slickDataSource) { services =>
        withStatsD {
          Get("/notifications/workspace/foo/bar") ~>
            services.sealedInstrumentedRoutes ~>
            check {
              assertResult(StatusCodes.OK) {
                status
              }
              val results = responseAs[Seq[Map[String, String]]]
              //          assert(!results.isEmpty)
              results.foreach { map =>
                val notificationKey = map.getOrElse("notificationKey", fail("notificationKey missing"))
                assert(notificationKey.startsWith("notifications/"))
                assert(notificationKey.endsWith("/foo/bar"))
                assert(map.isDefinedAt("description"))
              }
            }
        } { capturedMetrics =>
          val wsPathForRequestMetrics = "notifications.workspace.redacted.redacted"
          val expected = expectedHttpRequestMetrics("get", wsPathForRequestMetrics, StatusCodes.OK.intValue, 1)
          assertSubsetOf(expected, capturedMetrics)
        }
      }
  }

  it should "get general notifications" in withEmptyTestDatabase { slickDataSource: SlickDataSource =>
    withApiServices(slickDataSource) { services =>
      withStatsD {
        Get("/notifications/general") ~>
          services.sealedInstrumentedRoutes ~>
          check {
            assertResult(StatusCodes.OK) {
              status
            }
            val results = responseAs[Seq[Map[String, String]]]
            assert(!results.isEmpty)
            results.foreach { map =>
              val notificationKey = map.getOrElse("notificationKey", fail("notificationKey missing"))
              assert(notificationKey.startsWith("notifications/"))
              assert(map.isDefinedAt("description"))
            }
          }
      } { capturedMetrics =>
        val wsPathForRequestMetrics = "notifications.general"
        val expected = expectedHttpRequestMetrics("get", wsPathForRequestMetrics, StatusCodes.OK.intValue, 1)
        assertSubsetOf(expected, capturedMetrics)
      }
    }
  }

}
