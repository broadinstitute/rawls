package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService

import scala.concurrent.ExecutionContext

/**
  * Created by dvoet on 11/2/2020.
  */

trait BillingApiServiceV2 extends UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  import PerRequest.requestCompleteMarshaller
  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._

  val userServiceConstructor: UserInfo => UserService

  val billingRoutesV2: server.Route = requireUserInfo() { userInfo =>
    pathPrefix("billing" / "v2") {
      pathPrefix(Segment) { projectId =>
        pathEnd {
          get {
            complete {
              userServiceConstructor(userInfo).getBillingProject(RawlsBillingProjectName(projectId))
            }
          } ~
            delete {
              complete {
                userServiceConstructor(userInfo).deleteBillingProject(RawlsBillingProjectName(projectId))
              }
            }
        } ~
          pathPrefix("members") {
            pathEnd {
              get {
                complete {
                  userServiceConstructor(userInfo).getBillingProjectMembers(RawlsBillingProjectName(projectId))
                }
              }
            } ~
              // these routes are for adding/removing users from projects
              path(Segment / Segment) { (workbenchRole, userEmail) =>
                put {
                  complete {
                    userServiceConstructor(userInfo).addUserToBillingProject(RawlsBillingProjectName(projectId), ProjectAccessUpdate(userEmail, ProjectRoles.withName(workbenchRole)))
                  }
                } ~
                  delete {
                    complete {
                      userServiceConstructor(userInfo).removeUserFromBillingProject(RawlsBillingProjectName(projectId), ProjectAccessUpdate(userEmail, ProjectRoles.withName(workbenchRole)))
                    }
                  }
              }
          }
      } ~
      pathEnd {
        get {
          complete { userServiceConstructor(userInfo).listBillingProjectsV2() }
        } ~
        post {
          entity(as[CreateRawlsBillingProjectFullRequest]) { createProjectRequest =>
            complete {
              userServiceConstructor(userInfo).createBillingProjectV2(createProjectRequest)
            }
          }
        }
      }
    }
  }
}
