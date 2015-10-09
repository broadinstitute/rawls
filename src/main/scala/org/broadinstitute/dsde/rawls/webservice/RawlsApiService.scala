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
import spray.http.StatusCodes._

import scala.concurrent.ExecutionContext
import scala.reflect.runtime.universe._

object RawlsApiServiceActor {
  def props(workspaceServiceConstructor: UserInfo => WorkspaceService)(implicit executionContext: ExecutionContext): Props = {
    Props(new RawlsApiServiceActor(workspaceServiceConstructor))
  }
}

class RawlsApiServiceActor(val workspaceServiceConstructor: UserInfo => WorkspaceService)(implicit val executionContext: ExecutionContext) extends Actor
  with RootRawlsApiService with WorkspaceApiService with EntityApiService with MethodConfigApiService with SubmissionApiService
  with AdminApiService with StandardUserInfoDirectives {

  def actorRefFactory = context
  def possibleRoutes = options{ complete(OK) } ~ baseRoute ~ workspaceRoutes ~ entityRoutes ~ methodConfigRoutes ~ submissionRoutes ~ swaggerRoute ~ adminRoutes ~
    get {
      pathPrefix("swagger") {
        pathEnd {
          getFromResource("swagger/index.html")
        } ~
        pathSingleSlash {
          complete {
            HttpResponse(StatusCodes.NotFound)
          }
        }
      } ~ getFromResourceDirectory("swagger/") ~ getFromResourceDirectory("META-INF/resources/webjars/swagger-ui/2.1.1/")
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
