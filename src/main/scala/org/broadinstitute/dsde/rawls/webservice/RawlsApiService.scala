package org.broadinstitute.dsde.rawls.webservice

import akka.actor.{Actor, ActorRefFactory, Props}
import com.gettyimages.spray.swagger.SwaggerHttpService
import com.wordnik.swagger.model.ApiInfo
import org.broadinstitute.dsde.rawls.model.UserInfo
import org.broadinstitute.dsde.rawls.openam.StandardUserInfoDirectives
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.http.MediaTypes._
import spray.http.{HttpResponse, StatusCodes}
import spray.routing.Directive.pimpApply
import spray.routing._

import scala.reflect.runtime.universe._

object RawlsApiServiceActor {
  def props(workspaceServiceConstructor: UserInfo => WorkspaceService): Props = {
    Props(new RawlsApiServiceActor(workspaceServiceConstructor))
  }
}

class RawlsApiServiceActor(val workspaceServiceConstructor: UserInfo => WorkspaceService) extends Actor
  with RootRawlsApiService with WorkspaceApiService with EntityApiService with MethodConfigApiService with SubmissionApiService
  with StandardUserInfoDirectives {

  implicit def executionContext = actorRefFactory.dispatcher
  def actorRefFactory = context
  def possibleRoutes = baseRoute ~ workspaceRoutes ~ entityRoutes ~ methodConfigRoutes ~ submissionRoutes ~ swaggerRoute ~
    get {
      path("swagger") {
        getFromResource("swagger/index.html")
      } ~ getFromResourceDirectory("swagger/") ~ getFromResourceDirectory("META-INF/resources/webjars/swagger-ui/2.0.24/")
    } ~
    get {
      pathSingleSlash {
        complete {
          HttpResponse(StatusCodes.Unauthorized);
        }
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
