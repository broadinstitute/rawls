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

trait BillingApiService extends UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  import PerRequest.requestCompleteMarshaller
  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._

  val userServiceConstructor: UserInfo => UserService

  val billingRoutes: server.Route = requireUserInfo() { userInfo =>
    pathPrefix("billing" / Segment) { projectId =>
      path("members") {
        get {
          complete { userServiceConstructor(userInfo).getBillingProjectMembers(RawlsBillingProjectName(projectId)) }
        }
      } ~
        // these routes are for adding/removing users from projects
        path(Segment / Segment) { (workbenchRole, userEmail) =>
          put {
            complete { userServiceConstructor(userInfo).addUserToBillingProject(RawlsBillingProjectName(projectId), ProjectAccessUpdate(userEmail, ProjectRoles.withName(workbenchRole))) }
          } ~
            delete {
              complete { userServiceConstructor(userInfo).removeUserFromBillingProject(RawlsBillingProjectName(projectId), ProjectAccessUpdate(userEmail, ProjectRoles.withName(workbenchRole))) }
            }
        }
    } ~
    path("billing") {
      post {
        entity(as[CreateRawlsBillingProjectFullRequest]) { createProjectRequest =>
          complete { userServiceConstructor(userInfo).startBillingProjectCreation(createProjectRequest) }
        }
      }
    }
  }
}
