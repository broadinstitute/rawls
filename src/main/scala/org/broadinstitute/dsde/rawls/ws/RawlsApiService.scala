package org.broadinstitute.dsde.rawls.ws

import akka.actor.{Actor, ActorRefFactory, Props}
import com.gettyimages.spray.swagger.SwaggerHttpService
import com.wordnik.swagger.annotations._
import com.wordnik.swagger.model.ApiInfo
import org.broadinstitute.dsde.rawls.model.{Workspace, WorkspaceShort}
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.http.MediaTypes._
import spray.http.Uri
import spray.routing.Directive.pimpApply
import spray.routing._

import scala.reflect.runtime.universe._

object RawlsApiServiceActor {
  def props(swaggerService: SwaggerService, workspaceServiceConstructor: () => WorkspaceService): Props = {
    Props(new RawlsApiServiceActor(swaggerService, workspaceServiceConstructor))
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

class RawlsApiServiceActor(swaggerService: SwaggerService, val workspaceServiceConstructor: () => WorkspaceService) extends Actor with RootRawlsApiService with WorkspaceApiService {
  implicit def executionContext = actorRefFactory.dispatcher
  def actorRefFactory = context
  def possibleRoutes = baseRoute ~ swaggerService.routes ~ workspaceRoutes
  def receive = runRoute(possibleRoutes)
  def apiTypes = Seq(typeOf[RootRawlsApiService], typeOf[WorkspaceApiService])
}

@Api(value = "", description = "Rawls Base API", position = 1)
trait RootRawlsApiService extends HttpService {
  @ApiOperation(value = "Check if Rawls is alive",
    nickname = "poke",
    httpMethod = "GET",
    produces = "text/html",
    response = classOf[String])
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Successful Request"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def baseRoute = {
    path("") {
      get {
        respondWithMediaType(`text/html`) {
          complete {
            <html>
              <body>
                <h1>Rawls web service is operational</h1>
              </body>
            </html>
          }
        }
      }
    } ~
    path("headers") {
      get {
        requestContext => requestContext.complete(requestContext.request.headers.mkString(",\n"))
      }
    }
  }
}

@Api(value = "workspace", description = "APIs for Workspace CRUD", position = 1)
trait WorkspaceApiService extends HttpService with PerRequestCreator {
  import spray.httpx.SprayJsonSupport._
  import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._

  val workspaceServiceConstructor: () => WorkspaceService

  val workspaceRoutes = putWorkspaceRoute ~ listWorkspacesRoute

  @ApiOperation(value = "Create/replace workspace",
    nickname = "create",
    httpMethod = "PUT")
  @ApiResponses(Array(
    new ApiResponse(code = 201, message = "Successful Request"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def putWorkspaceRoute = cookie("iPlanetDirectoryPro") { securityTokenCookie =>
    path("workspaces") {
      post {
        entity(as[Workspace]) { workspace =>
          requestContext => perRequest(requestContext,
            WorkspaceService.props(workspaceServiceConstructor),
            WorkspaceService.SaveWorkspace(workspace, requestContext.request.uri.copy(path = Uri.Path.Empty)))
        }
      }
    }
  }

  @ApiOperation(value = "List workspaces",
    nickname = "list",
    httpMethod = "GET",
    produces = "application/json",
    response = classOf[Seq[WorkspaceShort]])
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Successful Request"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def listWorkspacesRoute = cookie("iPlanetDirectoryPro") { securityTokenCookie =>
    path("workspaces") {
      get {
        requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor), WorkspaceService.ListWorkspaces)
      }
    }
  }
}
