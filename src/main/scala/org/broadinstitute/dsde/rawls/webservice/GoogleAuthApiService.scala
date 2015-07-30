package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.model.UserInfo
import org.broadinstitute.dsde.rawls.openam.OpenAmDirectives
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.http.MediaType
import spray.routing.HttpService

trait GoogleAuthApiService extends HttpService with PerRequestCreator with OpenAmDirectives {
  lazy private implicit val executionContext = actorRefFactory.dispatcher

  val workspaceServiceConstructor: UserInfo => WorkspaceService

  val callbackPath = "authentication/register_callback"

  val authRoutes = userInfoFromCookie() { userInfo =>
    path("authentication" / "register") {
      get {
        requestContext => perRequest(requestContext,
          WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.RegisterUser(callbackPath)
        )
      }
    } ~
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
