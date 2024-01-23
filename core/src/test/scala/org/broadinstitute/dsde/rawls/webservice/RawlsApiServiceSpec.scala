package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import org.broadinstitute.dsde.rawls.dataaccess.{HttpExecutionServiceDAO, MockShardedExecutionServiceCluster}
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.ExecutionServiceVersionFormat
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport.ApplicationVersionFormat
import org.broadinstitute.dsde.rawls.model.{ApplicationVersion, ExecutionServiceVersion}

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
        sealRoute(instrumentRequest(_ => versionRoutes)) ~>
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
}
