package org.broadinstitute.dsde.rawls.ws

import org.scalatest.{FlatSpec, Matchers}
import spray.testkit.ScalatestRouteTest

import scala.concurrent.duration._

class RawlsApiServiceSpec extends FlatSpec with RootRawlsApiService with ScalatestRouteTest with Matchers {
  def actorRefFactory = system

  implicit val routeTestTimeout = RouteTestTimeout(5 second)

  "Rawls" should "return a greeting for GET requests to the root path" in {
    Get() ~> baseRoute ~> check {
      responseAs[String] should include("Rawls web service is operational")
    }
  }
}