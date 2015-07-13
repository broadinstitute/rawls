package org.broadinstitute.dsde.rawls.webservice

import javax.ws.rs.Path

import akka.actor.{Actor, ActorRefFactory, Props}
import com.gettyimages.spray.swagger.SwaggerHttpService
import com.wordnik.swagger.annotations._
import com.wordnik.swagger.model.ApiInfo
import org.broadinstitute.dsde.rawls.openam.{RawlsOpenAmClient, StandardOpenAmDirectives}
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.routing.Directive.pimpApply
import spray.routing._

import scala.reflect.runtime.universe._

object RawlsApiServiceActor {
  def props(swaggerService: SwaggerService, workspaceServiceConstructor: () => WorkspaceService, rawlsOpenAmClient: RawlsOpenAmClient): Props = {
    Props(new RawlsApiServiceActor(swaggerService, workspaceServiceConstructor, rawlsOpenAmClient))
  }
}

class SwaggerService(override val apiVersion: String,
                     override val baseUrl: String,
                     override val docsPath: String,
                     override val swaggerVersion: String,
                     override val apiTypes: Seq[Type],
                     override val apiInfo: Option[ApiInfo])
  (implicit val actorRefFactory: ActorRefFactory)
  extends SwaggerHttpService

class RawlsApiServiceActor(swaggerService: SwaggerService, val workspaceServiceConstructor: () => WorkspaceService, val rawlsOpenAmClient: RawlsOpenAmClient) extends Actor
  with RootRawlsApiService with WorkspaceApiService with EntityApiService with MethodConfigApiService with SubmissionApiService with GoogleAuthApiService
  with StandardOpenAmDirectives {

  implicit def executionContext = actorRefFactory.dispatcher
  def actorRefFactory = context
  def possibleRoutes = baseRoute ~ workspaceRoutes ~ entityRoutes ~ methodConfigRoutes ~ submissionRoutes ~ authRoutes ~ swaggerService.routes ~
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
}
