package org.broadinstitute.dsde.rawls.webservice

/**
 * Created by tsharpe on 9/25/15.
 */

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.json.DefaultJsonProtocol._

import scala.concurrent.ExecutionContext

trait AdminApiService extends UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._

  val workspaceServiceConstructor: UserInfo => WorkspaceService
  val userServiceConstructor: UserInfo => UserService

  val adminRoutes: server.Route = requireUserInfo() { userInfo =>
    path("admin" / "billing" / Segment) { (projectId) =>
      delete {
        entity(as[Map[String, String]]) { ownerInfo =>
          complete {
            userServiceConstructor(userInfo).adminDeleteBillingProject(RawlsBillingProjectName(projectId), ownerInfo).map(_ => StatusCodes.NoContent)
          }
        }
      }
    } ~
    path("admin" / "project" / "registration") {
      post {
        entity(as[RawlsBillingProjectTransfer]) { xfer =>
          complete { userServiceConstructor(userInfo).adminRegisterBillingProject(xfer).map(_ => StatusCodes.Created) }
        }
      }
    } ~
    path("admin" / "project" / "registration" / Segment) { (projectName) =>
      delete {
        entity(as[Map[String, String]]) { ownerInfo =>
          complete { userServiceConstructor(userInfo).adminUnregisterBillingProjectWithOwnerInfo(RawlsBillingProjectName(projectName), ownerInfo).map(_ => StatusCodes.NoContent) }
        }
      }
    } ~
    path("admin" / "submissions") {
      get {
        complete { workspaceServiceConstructor(userInfo).adminListAllActiveSubmissions() }
      }
    } ~
    path("admin" / "submissions" / Segment / Segment / Segment) { (workspaceNamespace, workspaceName, submissionId) =>
      delete {
        complete {
          workspaceServiceConstructor(userInfo).adminAbortSubmission(WorkspaceName(workspaceNamespace, workspaceName), submissionId).map { count =>
            if(count == 1) StatusCodes.NoContent -> None
            else StatusCodes.NotFound -> Option(ErrorReport(StatusCodes.NotFound, s"Unable to abort submission. Submission ${submissionId} could not be found."))
          }
        }
      }
    } ~
    path("admin" / "submissions" / "queueStatusByUser") {
      get {
        complete { workspaceServiceConstructor(userInfo).adminWorkflowQueueStatusByUser }
      }
    } ~
    path("admin" / "user" / "role" / "curator" / Segment) { (userEmail) =>
      put {
        complete { userServiceConstructor(userInfo).adminAddLibraryCurator(RawlsUserEmail(userEmail)).map(_ => StatusCodes.OK) }
      } ~
      delete {
        complete { userServiceConstructor(userInfo).adminRemoveLibraryCurator(RawlsUserEmail(userEmail)).map(_ => StatusCodes.OK) }
      }
    } ~
    path("admin" / "workspaces") {
      get {
        parameters('attributeName.?, 'valueString.?, 'valueNumber.?, 'valueBoolean.?) { (nameOption, stringOption, numberOption, booleanOption) =>
          val resultFuture = nameOption match {
            case None => workspaceServiceConstructor(userInfo).listAllWorkspaces()
            case Some(attributeName) =>
              val name = AttributeName.fromDelimitedName(attributeName)
              (stringOption, numberOption, booleanOption) match {
                case (Some(string), None, None) => workspaceServiceConstructor(userInfo).adminListWorkspacesWithAttribute(name, AttributeString(string))
                case (None, Some(number), None) => workspaceServiceConstructor(userInfo).adminListWorkspacesWithAttribute(name, AttributeNumber(number.toDouble))
                case (None, None, Some(boolean)) => workspaceServiceConstructor(userInfo).adminListWorkspacesWithAttribute(name, AttributeBoolean(boolean.toBoolean))
                case _ => throw new RawlsException("Specify exactly one of valueString, valueNumber, or valueBoolean")
              }
          }
         complete { resultFuture }
        }
      }
    } ~
    path("admin" / "workspaces" / Segment / Segment / "flags") { (workspaceNamespace, workspaceName) =>
        get {
          complete {
            workspaceServiceConstructor(userInfo).adminListWorkspaceFeatureFlags(WorkspaceName(workspaceNamespace, workspaceName))
          }
        } ~
        put {
          entity(as[List[String]]) { flagNames =>
            complete {
              workspaceServiceConstructor(userInfo).adminOverwriteWorkspaceFeatureFlags(WorkspaceName(workspaceNamespace, workspaceName), flagNames)
            }
          }
        }
      }
  }
}
