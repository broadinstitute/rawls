package org.broadinstitute.dsde.rawls.webservice

import akka.actor.{Actor, ActorRefFactory, Props}
import com.gettyimages.spray.swagger.SwaggerHttpService
import com.wordnik.swagger.model.ApiInfo
import org.broadinstitute.dsde.rawls.model.UserInfo
import org.broadinstitute.dsde.rawls.openam.StandardUserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.http.MediaTypes._
import spray.http.{HttpResponse, StatusCodes}
import spray.routing.Directive.pimpApply
import spray.routing._
import spray.http.StatusCodes._

import scala.concurrent.ExecutionContext
import scala.reflect.runtime.universe._

object RawlsApiServiceActor {
  def props(workspaceServiceConstructor: UserInfo => WorkspaceService, userServiceConstructor: UserInfo => UserService)(implicit executionContext: ExecutionContext): Props = {
    Props(new RawlsApiServiceActor(workspaceServiceConstructor, userServiceConstructor))
  }
}

class RawlsApiServiceActor(val workspaceServiceConstructor: UserInfo => WorkspaceService, val userServiceConstructor: UserInfo => UserService)(implicit val executionContext: ExecutionContext) extends Actor
  with RootRawlsApiService with WorkspaceApiService with EntityApiService with MethodConfigApiService with SubmissionApiService
  with AdminApiService with UserApiService with StandardUserInfoDirectives {

  def actorRefFactory = context
  def apiRoutes = options{ complete(OK) } ~ baseRoute ~ workspaceRoutes ~ entityRoutes ~ methodConfigRoutes ~ submissionRoutes ~ adminRoutes ~ userRoutes
  def registerRoutes = options{ complete(OK) } ~ createUserRoute ~ getUserStatusRoute

  val swaggerRoute = {
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
  }

  def receive = runRoute(
    swaggerRoute ~
    pathPrefix("api") {
      apiRoutes
    } ~
    pathPrefix("register") {
      registerRoutes
    }
  )
}

trait RootRawlsApiService extends HttpService {
  def baseRoute = {
    path("headers") {
      get {
        requestContext => requestContext.complete(requestContext.request.headers.mkString(",\n"))
      }
    }
  }
}
