package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import org.broadinstitute.dsde.rawls.metrics.TracingDirectives
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService
import spray.json.DefaultJsonProtocol._

import scala.concurrent.ExecutionContext

/**
  * Created by dvoet on 6/4/15.
  */

trait BillingApiService extends UserInfoDirectives with TracingDirectives {
  implicit val executionContext: ExecutionContext

  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._

  val userServiceConstructor: RawlsRequestContext => UserService

  val billingRoutes: server.Route = traceRequest { otelContext =>
    requireUserInfo(Option(otelContext)) { userInfo =>
      val ctx = RawlsRequestContext(userInfo, Option(otelContext))
      pathPrefix("billing" / Segment) { projectId =>
        path("members") {
          get {
            complete {
              userServiceConstructor(ctx).getBillingProjectMembers(RawlsBillingProjectName(projectId))
            }
          }
        } ~
          // these routes are for adding/removing users from projects
          path(Segment / Segment) { (workbenchRole, userEmail) =>
            put {
              complete {
                userServiceConstructor(ctx)
                  .addUserToBillingProject(RawlsBillingProjectName(projectId),
                                           ProjectAccessUpdate(userEmail, ProjectRoles.withName(workbenchRole))
                  )
                  .map(_ => StatusCodes.OK)
              }
            } ~
              delete {
                complete {
                  userServiceConstructor(ctx)
                    .removeUserFromBillingProject(RawlsBillingProjectName(projectId),
                                                  ProjectAccessUpdate(userEmail, ProjectRoles.withName(workbenchRole))
                    )
                    .map(_ => StatusCodes.OK)
                }
              }
          }
      }
    }
  }
}
