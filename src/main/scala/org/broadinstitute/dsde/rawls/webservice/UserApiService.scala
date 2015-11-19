package org.broadinstitute.dsde.rawls.webservice

import kamon.spray.KamonTraceDirectives._
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
        traceName("CreateUser") {
          requestContext => perRequest(requestContext,
            UserService.props(userServiceConstructor, userInfo),
            UserService.CreateUser)
        }
      }
    }
  }

  val userRoutes = requireUserInfo() { userInfo =>
    path("user" / "refreshToken") {
      put {
        traceName("SetRefreshToken") {
          entity(as[UserRefreshToken]) { token =>
            requestContext => perRequest(requestContext,
              UserService.props(userServiceConstructor, userInfo),
              UserService.SetRefreshToken(token))
          }
        }
      }
    } ~
    path("user" / "refreshTokenDate") {
      get {
        traceName("GetRefreshTokenDate") {
          requestContext => perRequest(requestContext,
            UserService.props(userServiceConstructor, userInfo),
            UserService.GetRefreshTokenDate)
        }
      }
    } ~
    path("user" / "billing") {
      get {
        traceName("ListBillingProjects") {
          requestContext => perRequest(requestContext,
            UserService.props(userServiceConstructor, userInfo),
            UserService.ListBillingProjects)
        }
      }
    } ~
    path("user" / Segment) { userSubjectId =>
      get {
        traceName("GetUserStatus") {
          requestContext => perRequest(requestContext,
            UserService.props(userServiceConstructor, userInfo),
            UserService.GetUserStatus(RawlsUserRef(RawlsUserSubjectId(userSubjectId))))
        }
      }
    } ~
    path("user" / Segment / "enable") { userSubjectId =>
      post {
        traceName("EnableUser") {
          requestContext => perRequest(requestContext,
            UserService.props(userServiceConstructor, userInfo),
            UserService.EnableUser(RawlsUserRef(RawlsUserSubjectId(userSubjectId))))
        }
      }
    } ~
    path("user" / Segment / "disable") { userSubjectId =>
      post {
        traceName("DisableUser") {
          requestContext => perRequest(requestContext,
            UserService.props(userServiceConstructor, userInfo),
            UserService.DisableUser(RawlsUserRef(RawlsUserSubjectId(userSubjectId))))
        }
      }
    }
  }
}
