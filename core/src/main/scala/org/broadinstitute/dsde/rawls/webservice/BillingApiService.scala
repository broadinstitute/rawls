package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.workbench.model.{WorkbenchEmail, WorkbenchUserId}
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
  * Created by dvoet on 6/4/15.
  */

trait BillingApiService extends UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._
  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import PerRequest.requestCompleteMarshaller

  val userServiceConstructor: UserInfo => UserService

  val billingRoutes: server.Route = requireUserInfo() { userInfo =>
    pathPrefix("billing" / Segment) { projectId =>
      path("members") {
        get {
          complete { userServiceConstructor(userInfo).GetBillingProjectMembers(RawlsBillingProjectName(projectId)) }
        }
      } ~
        // these routes are for setting/unsetting Google cloud roles
        path("googleRole" / Segment / Segment) { (googleRole, userEmail) =>
          put {
            complete { userServiceConstructor(userInfo).GrantGoogleRoleToUser(RawlsBillingProjectName(projectId), WorkbenchEmail(userEmail), googleRole) }
          } ~
            delete {
              complete { userServiceConstructor(userInfo).RemoveGoogleRoleFromUser(RawlsBillingProjectName(projectId), WorkbenchEmail(userEmail), googleRole) }
            }
        } ~
        // these routes are for adding/removing users from projects
        path(Segment / Segment) { (workbenchRole, userEmail) =>
          put {
            complete { userServiceConstructor(userInfo).AddUserToBillingProject(RawlsBillingProjectName(projectId), ProjectAccessUpdate(userEmail, ProjectRoles.withName(workbenchRole))) }
          } ~
            delete {
              complete { userServiceConstructor(userInfo).RemoveUserFromBillingProject(RawlsBillingProjectName(projectId), ProjectAccessUpdate(userEmail, ProjectRoles.withName(workbenchRole))) }
            }
        }
    } ~
    path("billing") {
      post {
        entity(as[CreateRawlsBillingProjectFullRequest]) { createProjectRequest =>
          complete { userServiceConstructor(userInfo).CreateBillingProjectFull(createProjectRequest.projectName, createProjectRequest.billingAccount, createProjectRequest.highSecurityNetwork.getOrElse(false)) }
        }
      }
    }
  }
}
