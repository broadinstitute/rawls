package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService

import scala.concurrent.ExecutionContext

/**
  * Created by dvoet on 6/4/15.
  */

trait UserApiService extends UserInfoDirectives {
  import PerRequest.requestCompleteMarshaller
  implicit val executionContext: ExecutionContext

  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import org.broadinstitute.dsde.rawls.model.UserJsonSupport._

  val userServiceConstructor: UserInfo => UserService

  // standard /api routes begin here

  val userRoutes: server.Route = requireUserInfo() { userInfo =>
    path("user" / "refreshToken") {
      put {
        entity(as[UserRefreshToken]) { token =>
          complete { userServiceConstructor(userInfo).setRefreshToken(token) }
        }
      }
    } ~
      path("user" / "refreshTokenDate") {
        get {
          complete { userServiceConstructor(userInfo).getRefreshTokenDate() }
        }
      } ~
      pathPrefix("user" / "billing") {
        pathEnd {
          get {
            complete { userServiceConstructor(userInfo).listBillingProjects() }
          }
        } ~
        path(Segment) { projectName =>
          get {
            complete { userServiceConstructor(userInfo).getBillingProjectStatus(RawlsBillingProjectName(projectName)) }
          }
        } ~
        path(Segment) { projectName =>
          delete {
            complete { userServiceConstructor(userInfo).deleteBillingProject(RawlsBillingProjectName(projectName)) }
          }
        }
      } ~
      path("user" / "role" / "admin") {
        get {
          complete { userServiceConstructor(userInfo).isAdmin(userInfo.userEmail) }
        }
      } ~
      path("user" / "role" / "curator") {
        get {
          complete { userServiceConstructor(userInfo).isLibraryCurator(userInfo.userEmail) }
        }
      } ~
      path("user" / "billingAccounts") {
        get {
          complete { userServiceConstructor(userInfo).listBillingAccounts() }
        }
      }
  }
}
