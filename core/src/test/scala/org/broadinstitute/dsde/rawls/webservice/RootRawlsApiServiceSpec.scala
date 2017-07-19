package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.model.ApplicationVersion
import spray.http.StatusCodes
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport.ApplicationVersionFormat

/**
 * Created by dvoet on 1/26/16.
 */
class RootRawlsApiServiceSpec extends ApiServiceSpec with RootRawlsApiService {
  override val appVersion = ApplicationVersion("githash", "buildnumber", "version")
  override val googleClientId = "FAKE-VALUE"

  "RootRawlsApiService" should "get a version" in  {
    withStatsD {
      Get("/version") ~>
        sealRoute(instrumentRequest {versionRoute} ) ~>
        check {
          assertResult(StatusCodes.OK) {status}
          assertResult(appVersion) {responseAs[ApplicationVersion]}
        }
    } { capturedMetrics =>
      val expected = expectedHttpRequestMetrics("get", "version", StatusCodes.OK.intValue, 1)
      assert {
        expected subsetOf capturedMetrics.toSet
      }
    }
  }
}
