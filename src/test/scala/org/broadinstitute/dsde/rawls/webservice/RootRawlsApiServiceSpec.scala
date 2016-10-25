package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.model.ApplicationVersion
import spray.http.StatusCodes
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport.ApplicationVersionFormat

/**
 * Created by dvoet on 1/26/16.
 */
class RootRawlsApiServiceSpec extends ApiServiceSpec with RootRawlsApiService {
  val appVersion = ApplicationVersion("githash", "buildnumber", "version")
  val googleClientId = "FAKE-VALUE"
  val swaggerUIVersion = "FAKE-VALUE"

  "RootRawlsApiService" should "get a version" in  {
    Get("/version") ~>
      sealRoute(versionRoute) ~>
      check {
        assertResult(StatusCodes.OK) {status}
        assertResult(appVersion) {responseAs[ApplicationVersion]}
      }
  }
}
