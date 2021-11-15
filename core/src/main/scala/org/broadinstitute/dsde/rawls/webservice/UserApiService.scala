package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService
import spray.json.DefaultJsonProtocol._

import scala.concurrent.ExecutionContext

/**
  * Created by dvoet on 6/4/15.
  */

trait UserApiService extends UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import org.broadinstitute.dsde.rawls.model.UserJsonSupport._

  val userServiceConstructor: UserInfo => UserService

  // standard /api routes begin here

  val userRoutes: server.Route = requireUserInfo() { userInfo =>
    path("user" / "refreshToken") {
      put {
        entity(as[UserRefreshToken]) { token =>
          complete {
            userServiceConstructor(userInfo).setRefreshToken(token).map(_ => StatusCodes.Created)
          }
        }
      }
    } ~
      path("user" / "refreshTokenDate") {
        get {
          complete {
            userServiceConstructor(userInfo).getRefreshTokenDate().map(StatusCodes.OK -> _)
          }
        }
      } ~
      pathPrefix("user" / "billing") {
        pathEnd {
          get {
            complete {
              userServiceConstructor(userInfo).listBillingProjects().map(StatusCodes.OK -> _)
            }
          }
        } ~
        path(Segment) { projectName =>
          get {
            complete {
              userServiceConstructor(userInfo).getBillingProjectStatus(RawlsBillingProjectName(projectName)).map {
                case Some(status) => StatusCodes.OK -> Right(Option(status))
                case _ => StatusCodes.NotFound -> Left(Option(StatusCodes.NotFound.defaultMessage))
              }
            }
          }
        } ~
        path(Segment) { projectName =>
          delete {
            complete {
              userServiceConstructor(userInfo).deleteBillingProject(RawlsBillingProjectName(projectName)).map(_ => StatusCodes.NoContent)
            }
          }
        }
      } ~
      path("user" / "role" / "admin") {
        get {
          complete {
            userServiceConstructor(userInfo).isAdmin(userInfo.userEmail).map {
              case true => StatusCodes.OK
              case false => StatusCodes.NotFound
            }
          }
        }
      } ~
      path("user" / "role" / "curator") {
        get {
          complete {
            userServiceConstructor(userInfo).isLibraryCurator(userInfo.userEmail).map {
              case true => StatusCodes.OK
              case false => StatusCodes.NotFound
            }
          }
        }
      } ~
      path("user" / "billingAccounts") {
        get {
          complete { userServiceConstructor(userInfo).listBillingAccounts().map(StatusCodes.OK -> _) }
        }
      }
  }
}
