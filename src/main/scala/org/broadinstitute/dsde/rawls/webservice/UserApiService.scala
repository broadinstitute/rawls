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

trait UserApiService extends HttpService with PerRequestCreator with UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  import org.broadinstitute.dsde.rawls.model.UserJsonSupport._
  import spray.httpx.SprayJsonSupport._

  val userServiceConstructor: UserInfo => UserService

  // special route for registration that has different access control
  val createUserRoute = requireUserInfo() { userInfo =>
    path("user") {
      post {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.CreateUser)
      }
    }
  }

  val getUserStatusRoute = requireUserInfo() { userInfo =>
    path("user") {
      get {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.UserGetUserStatus)
      }
    }
  }

  val userRoutes = requireUserInfo() { userInfo =>
    path("user" / "refreshToken") {
      put {
        entity(as[UserRefreshToken]) { token =>
          requestContext => perRequest(requestContext,
            UserService.props(userServiceConstructor, userInfo),
            UserService.SetRefreshToken(token))
        }
      }
    } ~
    path("user" / "refreshTokenDate") {
      get {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.GetRefreshTokenDate)
      }
    } ~
    path("user" / "billing") {
      get {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.ListBillingProjects)
      }
    } ~
    path("user" / "billingAccounts") {
      get {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.ListBillingAccounts)
      }
    } ~
    path("user" / "group" / Segment) { groupName =>
      get {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.GetUserGroup(RawlsGroupRef(RawlsGroupName(groupName))))
      }
    }
  }
}
