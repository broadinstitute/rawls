package org.broadinstitute.dsde.rawls

import org.broadinstitute.dsde.rawls.model.{WorkspaceJsonSupport, WorkspaceRequest}
import org.broadinstitute.dsde.rawls.webservice.WorkspaceApiService
import org.joda.time.DateTime
import spray.http._
import spray.httpx.SprayJsonSupport
import SprayJsonSupport._
import WorkspaceJsonSupport._

import spray.http.StatusCodes

import scala.concurrent.duration._

class GcsBucketTest extends IntegrationTestBase with WorkspaceApiService {
  implicit val routeTestTimeout = RouteTestTimeout(5.seconds)
  def actorRefFactory = system

  val workspaceServiceConstructor = workspaceServiceWithDbName("gcs-bucket-test-latest") // TODO move this into config?

  val workspace = WorkspaceRequest(
    "namespace",
    "name",
    Map.empty
  )

  "GcsBucketTest" should "create a bucket for a workspace" in {
    Post(s"/workspaces", httpJson(workspace)) ~>
      addOpenAmCookie ~>
      sealRoute(postWorkspaceRoute) ~>
      check {
        println(response)
      }
  }
}
