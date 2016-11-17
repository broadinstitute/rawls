package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService
import spray.routing.Directive.pimpApply
import spray.routing._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
 * Created by dvoet on 6/4/15.
 */

trait BillingApiService extends HttpService with PerRequestCreator with UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._
  import spray.httpx.SprayJsonSupport._

  val userServiceConstructor: UserInfo => UserService

  val billingRoutes = requireUserInfo() { userInfo =>
    path("billing" / Segment / "members") { projectId =>
      get {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.GetBillingProjectMembers(RawlsBillingProjectName(projectId)))
        }
    } ~
    path("billing" / Segment / Segment / Segment) { (projectId, role, userEmail) =>
      put {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.AddUserToBillingProject(RawlsBillingProjectName(projectId), ProjectAccessUpdate(userEmail, ProjectRoles.withName(role))))
      } ~
      delete {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.RemoveUserFromBillingProject(RawlsBillingProjectName(projectId), ProjectAccessUpdate(userEmail, ProjectRoles.withName(role))))
      }
    } ~
    path("billing") {
      post {
        entity(as[CreateRawlsBillingProjectFullRequest]) { createProjectRequest =>
          requestContest => perRequest(requestContest,
            UserService.props(userServiceConstructor, userInfo),
            UserService.CreateBillingProjectFull(createProjectRequest.projectName, createProjectRequest.billingAccount), 7 minutes) // it can take a while
        }
      }
    }
  }
}
