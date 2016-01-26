package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.model.ApplicationVersion
import org.scalatest.{FlatSpec, Matchers}
import spray.http.StatusCodes
import spray.httpx.SprayJsonSupport
import spray.routing.HttpService
import spray.testkit.ScalatestRouteTest
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport.ApplicationVersionFormat

/**
 * Created by dvoet on 1/26/16.
 */
class RootRawlsApiServiceSpec extends FlatSpec with HttpService with ScalatestRouteTest with Matchers with SprayJsonSupport with RootRawlsApiService {
  def actorRefFactory = system
  val appVersion = ApplicationVersion("githash", "buildnumber", "version")

  "RootRawlsApiService" should "get a version" in  {
    Get("/version") ~>
      sealRoute(versionRoute) ~>
      check {
        assertResult(StatusCodes.OK) {status}
        assertResult(appVersion) {responseAs[ApplicationVersion]}
      }
  }
}
