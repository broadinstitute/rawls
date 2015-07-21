package org.broadinstitute.dsde.rawls.webservice

import javax.ws.rs.Path

import com.wordnik.swagger.annotations._
import org.broadinstitute.dsde.rawls.model.UserInfo
import org.broadinstitute.dsde.rawls.openam.OpenAmDirectives
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.http.MediaType
import spray.routing.HttpService

@Api(value = "authentication", description = "Google authentication API")
trait GoogleAuthApiService extends HttpService with PerRequestCreator with OpenAmDirectives {
  lazy private implicit val executionContext = actorRefFactory.dispatcher

  val workspaceServiceConstructor: UserInfo => WorkspaceService
  val authRoutes =
    registerPostRoute ~
    registerCallbackRoute

  val callbackPath = "authentication/register_callback"

  @Path("/register")
  @ApiOperation(value = "Register new user.", httpMethod = "GET", consumes = "text/plain")
  @ApiResponses(Array(
    new ApiResponse(code = 303, message = "Successful Request -- Go Visit Google"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def registerPostRoute = userInfoFromCookie() { userInfo =>
    path("authentication" / "register") {
      get {
        requestContext => perRequest(requestContext,
          WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.RegisterUser(callbackPath)
        )
      }
    }
  }

  @Path("/register_callback")
  @ApiOperation(value = "Receive auth response from Google.", httpMethod = "GET")
  @ApiImplicitParams(Array(
      new ApiImplicitParam(name = "code", required = true, dataType = "string", paramType = "query", value = "Authorization code"),
      new ApiImplicitParam(name = "state", required = true, dataType = "string", paramType = "query", value = "State Passthrough")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 201, message = "Successful Request -- Registered New User"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def registerCallbackRoute = userInfoFromCookie() { userInfo =>
    path("authentication" / "register_callback") {
      get {
        parameters("code", "state") { (authCode, state) =>
          requestContext => perRequest(requestContext,
            WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.CompleteUserRegistration(authCode, state, callbackPath)
          )
        }
      }
    }
  }
}
