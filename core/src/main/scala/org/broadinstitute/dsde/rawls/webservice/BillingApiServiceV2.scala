package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService
import spray.json.DefaultJsonProtocol._

import scala.concurrent.ExecutionContext

/**
  * Created by dvoet on 11/2/2020.
  */

trait BillingApiServiceV2 extends UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._
  import spray.json.DefaultJsonProtocol._

  val userServiceConstructor: UserInfo => UserService

  val billingRoutesV2: server.Route = requireUserInfo() { userInfo =>
    pathPrefix("billing" / "v2") {
      pathPrefix(Segment) { projectId =>
        pathEnd {
          get {
            complete {
              import spray.json._
              userServiceConstructor(userInfo).getBillingProject(RawlsBillingProjectName(projectId)).map {
                case Some(projectResponse) => StatusCodes.OK -> Option(projectResponse).toJson
                case None => StatusCodes.NotFound -> Option(StatusCodes.NotFound.defaultMessage).toJson
              }
            }
          } ~
            delete {
              complete {
                userServiceConstructor(userInfo).deleteBillingProject(RawlsBillingProjectName(projectId)).map(_ => StatusCodes.NoContent)
              }
            }
        } ~
        pathPrefix("spendReportConfiguration") {
          pathEnd {
            put {
              entity(as[BillingProjectSpendConfiguration]) { spendConfiguration =>
                complete {
                  userServiceConstructor(userInfo).setBillingProjectSpendConfiguration(RawlsBillingProjectName(projectId), spendConfiguration).map(_ => StatusCodes.NoContent)
                }
              }
            } ~
            delete {
              complete {
                userServiceConstructor(userInfo).clearBillingProjectSpendConfiguration(RawlsBillingProjectName(projectId)).map(_ => StatusCodes.NoContent)
              }
            } ~
            get {
              complete {
                userServiceConstructor(userInfo).getBillingProjectSpendConfiguration(RawlsBillingProjectName(projectId)).map {
                  case Some(config) => StatusCodes.OK -> Option(config)
                  case None => StatusCodes.NoContent -> None
                }
              }
            }
          }
        } ~
        pathPrefix("billingAccount") {
          pathEnd {
            put {
              entity(as[UpdateRawlsBillingAccountRequest]) { updateProjectRequest =>
                complete{
                  userServiceConstructor(userInfo).updateBillingProjectBillingAccount(RawlsBillingProjectName(projectId), updateProjectRequest).map(_ => StatusCodes.OK)
                }
              }
            } ~
              delete {
                complete {
                  userServiceConstructor(userInfo).deleteBillingAccount(RawlsBillingProjectName(projectId)).map(_ => StatusCodes.OK)
                }
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
                  userServiceConstructor(userInfo).addUserToBillingProject(RawlsBillingProjectName(projectId), ProjectAccessUpdate(userEmail, ProjectRoles.withName(workbenchRole))).map(_ => StatusCodes.OK)
                }
              } ~
                delete {
                  complete {
                    userServiceConstructor(userInfo).removeUserFromBillingProject(RawlsBillingProjectName(projectId), ProjectAccessUpdate(userEmail, ProjectRoles.withName(workbenchRole))).map(_ => StatusCodes.OK)
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
          entity(as[CreateRawlsV2BillingProjectFullRequest]) { createProjectRequest =>
            complete {
              userServiceConstructor(userInfo).createBillingProjectV2(createProjectRequest).map(_ => StatusCodes.Created)
            }
          }
        }
      }
    }
  }
}
