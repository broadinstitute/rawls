package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService
import spray.routing.Directive.pimpApply
import spray.routing._

import scala.concurrent.ExecutionContext

/**
 * Created by dvoet on 6/4/15.
 */

trait BillingApiService extends HttpService with PerRequestCreator with UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  import org.broadinstitute.dsde.rawls.model.UserJsonSupport._
  import spray.httpx.SprayJsonSupport._

  val userServiceConstructor: UserInfo => UserService

  val billingRoutes = requireUserInfo() { userInfo =>
    path("billing" / Segment / Segment / Segment) { (projectId, role, userEmail) =>
      put {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.AddUserToBillingProject(RawlsBillingProjectName(projectId), RawlsUserEmail(userEmail), ProjectRoles.withName(role)))
      } ~
      delete {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.RemoveUserFromBillingProject(RawlsBillingProjectName(projectId), RawlsUserEmail(userEmail)))
      }
    }
  }
}
