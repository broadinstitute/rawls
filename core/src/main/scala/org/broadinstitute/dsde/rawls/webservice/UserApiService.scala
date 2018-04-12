package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import CustomDirectives._

import scala.concurrent.ExecutionContext

/**
  * Created by dvoet on 6/4/15.
  */

trait UserApiService extends UserInfoDirectives {
  import PerRequest.requestCompleteMarshaller
  implicit val executionContext: ExecutionContext

  import org.broadinstitute.dsde.rawls.model.UserJsonSupport._
  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._

  val userServiceConstructor: UserInfo => UserService

  // these routes have different access control and their paths have /register instead of /api

  val createUserRoute: server.Route = requireUserInfo() { userInfo =>
    path("user") {
      addLocationHeader(s"/user/${userInfo.userSubjectId.value}") {
        post {
          complete { userServiceConstructor(userInfo).CreateUser.map(_ => StatusCodes.Created) }
        }
      }
    }
  }

  // standard /api routes begin here

  val userRoutes: server.Route = requireUserInfo() { userInfo =>
    path("user" / "refreshToken") {
      put {
        entity(as[UserRefreshToken]) { token =>
          complete { userServiceConstructor(userInfo).SetRefreshToken(token) }
        }
      }
    } ~
      path("user" / "refreshTokenDate") {
        get {
          complete { userServiceConstructor(userInfo).GetRefreshTokenDate }
        }
      } ~
      path("user" / "billing") {
        get {
          complete { userServiceConstructor(userInfo).ListBillingProjects }
        }
      } ~
      path("user" / "role" / "admin") {
        get {
          complete { userServiceConstructor(userInfo).IsAdmin(userInfo.userEmail) }
        }
      } ~
      path("user" / "role" / "curator") {
        get {
          complete { userServiceConstructor(userInfo).IsLibraryCurator(userInfo.userEmail) }
        }
      } ~
      path("user" / "billingAccounts") {
        get {
          complete { userServiceConstructor(userInfo).ListBillingAccounts }
        }
      }
  }
}
