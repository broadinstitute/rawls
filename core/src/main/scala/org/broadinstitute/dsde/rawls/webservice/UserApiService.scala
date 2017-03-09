package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._
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

  // these routes have different access control and their paths have /register instead of /api

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

  // standard /api routes begin here

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
    path("user" / "role" / "admin") {
      get {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.IsAdmin(RawlsUserEmail(userInfo.userEmail)))
      }
    } ~
    path("user" / "role" / "curator") {
      get {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.IsLibraryCurator(RawlsUserEmail(userInfo.userEmail)))
      }
    } ~
    path("user" / "billingAccounts") {
      get {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.ListBillingAccounts)
      }
    } ~
    path("user" / "groups") {
      get {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.ListGroupsForUser(RawlsUserEmail(userInfo.userEmail)))
      }
    } ~
    path("user" / "group" / Segment) { groupName =>
      get {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.GetUserGroup(RawlsGroupRef(RawlsGroupName(groupName))))
      }
    } ~
    path("user" / "realms") {
      get {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.ListManagedGroupsForUser)
      }
    }
  }
}
