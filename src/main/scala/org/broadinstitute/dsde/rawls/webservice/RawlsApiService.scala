package org.broadinstitute.dsde.rawls.webservice

import akka.actor.{Actor, ActorRefFactory, Props}
import com.gettyimages.spray.swagger.SwaggerHttpService
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal
import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.blueprints.impls.orient.{OrientGraph, OrientEdge, OrientVertex}
import com.wordnik.swagger.model.ApiInfo
import org.broadinstitute.dsde.rawls.model.UserInfo
import org.broadinstitute.dsde.rawls.openam.{RawlsOpenAmClient, StandardOpenAmDirectives}
import org.broadinstitute.dsde.rawls.webservice.PerRequest.RequestComplete
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.http.MediaTypes._
import spray.http.StatusCodes
import spray.routing.Directive.pimpApply
import spray.routing._
import spray.httpx.SprayJsonSupport._

import scala.reflect.runtime.universe._

object RawlsApiServiceActor {
  def props(workspaceServiceConstructor: UserInfo => WorkspaceService, rawlsOpenAmClient: RawlsOpenAmClient): Props = {
    Props(new RawlsApiServiceActor(workspaceServiceConstructor, rawlsOpenAmClient))
  }
}

class RawlsApiServiceActor(val workspaceServiceConstructor: UserInfo => WorkspaceService, val rawlsOpenAmClient: RawlsOpenAmClient) extends Actor
  with RootRawlsApiService with WorkspaceApiService with EntityApiService with MethodConfigApiService with SubmissionApiService with GoogleAuthApiService
  with StandardOpenAmDirectives {

  implicit def executionContext = actorRefFactory.dispatcher
  def actorRefFactory = context
  def possibleRoutes = baseRoute ~ workspaceRoutes ~ entityRoutes ~ methodConfigRoutes ~ submissionRoutes ~ authRoutes ~ swaggerRoute ~
    get {
      pathSingleSlash {
        getFromResource("swagger/index.html")
      } ~ getFromResourceDirectory("swagger/") ~ getFromResourceDirectory("META-INF/resources/webjars/swagger-ui/2.0.24/")
    }

  def receive = runRoute(possibleRoutes)
}

trait RootRawlsApiService extends HttpService {
  def baseRoute = {
    path("headers") {
      get {
        requestContext => requestContext.complete(requestContext.request.headers.mkString(",\n"))
      }
    }
  }
  def docsPath: String = "api-docs" //path to swagger's endpoint
  def swaggerRoute = {
    get {
      path(docsPath / "listings" / Segment) { (listing) =>
        getFromResource("swagger/listings/" + listing)
      }
    }
  }
}
