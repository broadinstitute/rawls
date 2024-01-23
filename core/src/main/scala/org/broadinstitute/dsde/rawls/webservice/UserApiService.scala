package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import org.broadinstitute.dsde.rawls.metrics.TracingDirectives
import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService
import spray.json.DefaultJsonProtocol._

import scala.concurrent.ExecutionContext

/**
  * Created by dvoet on 6/4/15.
  */

trait UserApiService extends UserInfoDirectives with TracingDirectives {
  implicit val executionContext: ExecutionContext

  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._

  val userServiceConstructor: RawlsRequestContext => UserService

  // standard /api routes begin here

  val userRoutes: server.Route = traceRequest { otelContext =>
    requireUserInfo(Option(otelContext)) { userInfo =>
      val ctx = RawlsRequestContext(userInfo, Option(otelContext))
      pathPrefix("user" / "billing") {
        pathEnd {
          get {
            complete {
              userServiceConstructor(ctx).listBillingProjects()
            }
          }
        } ~
          path(Segment) { projectName =>
            get {
              complete {
                import spray.json._
                userServiceConstructor(ctx).getBillingProjectStatus(RawlsBillingProjectName(projectName)).map {
                  case Some(status) => StatusCodes.OK -> Option(status).toJson
                  case _            => StatusCodes.NotFound -> Option(StatusCodes.NotFound.defaultMessage).toJson
                }
              }
            }
          } ~
          path(Segment) { projectName =>
            delete {
              complete {
                userServiceConstructor(ctx)
                  .deleteBillingProject(RawlsBillingProjectName(projectName))
                  .map(_ => StatusCodes.NoContent)
              }
            }
          }
      } ~
        path("user" / "role" / "admin") {
          get {
            complete {
              userServiceConstructor(ctx).isAdmin(userInfo.userEmail).map {
                case true  => StatusCodes.OK
                case false => StatusCodes.NotFound
              }
            }
          }
        } ~
        path("user" / "role" / "curator") {
          get {
            complete {
              userServiceConstructor(ctx).isLibraryCurator(userInfo.userEmail).map {
                case true  => StatusCodes.OK
                case false => StatusCodes.NotFound
              }
            }
          }
        } ~
        path("user" / "billingAccounts") {
          get {
            parameters("firecloudHasAccess".as[Boolean].optional) { firecloudHasAccess =>
              complete {
                userServiceConstructor(ctx).listBillingAccounts(firecloudHasAccess)
              }
            }
          }
        }
    }
  }
}
