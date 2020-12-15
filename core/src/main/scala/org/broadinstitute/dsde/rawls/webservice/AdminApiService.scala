package org.broadinstitute.dsde.rawls.webservice

/**
 * Created by tsharpe on 9/25/15.
 */

import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.json.DefaultJsonProtocol._

import scala.concurrent.ExecutionContext

trait AdminApiService extends UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  import PerRequest.requestCompleteMarshaller
  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._

  val workspaceServiceConstructor: UserInfo => WorkspaceService
  val userServiceConstructor: UserInfo => UserService

  val adminRoutes: server.Route = requireUserInfo() { userInfo =>
    path("admin" / "billing" / Segment) { (projectId) =>
      delete {
        entity(as[Map[String, String]]) { ownerInfo =>
          complete {
            userServiceConstructor(userInfo).AdminDeleteBillingProject(RawlsBillingProjectName(projectId), ownerInfo)
          }
        }
      }
    } ~
    path("admin" / "project" / "registration") {
      post {
        entity(as[RawlsBillingProjectTransfer]) { xfer =>
          complete { userServiceConstructor(userInfo).AdminRegisterBillingProject(xfer) }
        }
      }
    } ~
    path("admin" / "project" / "registration" / Segment) { (projectName) =>
      delete {
        entity(as[Map[String, String]]) { ownerInfo =>
          complete { userServiceConstructor(userInfo).AdminUnregisterBillingProject(RawlsBillingProjectName(projectName), ownerInfo) }
        }
      }
    } ~
    path("admin" / "submissions") {
      get {
        complete { workspaceServiceConstructor(userInfo).AdminListAllActiveSubmissions }
      }
    } ~
    path("admin" / "submissions" / Segment / Segment / Segment) { (workspaceNamespace, workspaceName, submissionId) =>
      delete {
        complete { workspaceServiceConstructor(userInfo).AdminAbortSubmission(WorkspaceName(workspaceNamespace, workspaceName), submissionId) }
      }
    } ~
    path("admin" / "submissions" / "queueStatusByUser") {
      get {
        complete { workspaceServiceConstructor(userInfo).AdminWorkflowQueueStatusByUser }
      }
    } ~
    path("admin" / "user" / "role" / "curator" / Segment) { (userEmail) =>
      put {
        complete { userServiceConstructor(userInfo).AdminAddLibraryCurator(RawlsUserEmail(userEmail)) }
      } ~
      delete {
        complete { userServiceConstructor(userInfo).AdminRemoveLibraryCurator(RawlsUserEmail(userEmail)) }
      }
    } ~
    path("admin" / "workspaces") {
      get {
        parameters('attributeName.?, 'valueString.?, 'valueNumber.?, 'valueBoolean.?) { (nameOption, stringOption, numberOption, booleanOption) =>
          val resultFuture = nameOption match {
            case None => workspaceServiceConstructor(userInfo).ListAllWorkspaces
            case Some(attributeName) =>
              val name = AttributeName.fromDelimitedName(attributeName)
              (stringOption, numberOption, booleanOption) match {
                case (Some(string), None, None) => workspaceServiceConstructor(userInfo).AdminListWorkspacesWithAttribute(name, AttributeString(string))
                case (None, Some(number), None) => workspaceServiceConstructor(userInfo).AdminListWorkspacesWithAttribute(name, AttributeNumber(number.toDouble))
                case (None, None, Some(boolean)) => workspaceServiceConstructor(userInfo).AdminListWorkspacesWithAttribute(name, AttributeBoolean(boolean.toBoolean))
                case _ => throw new RawlsException("Specify exactly one of valueString, valueNumber, or valueBoolean")
              }
          }
         complete { resultFuture }
        }
      }
    }
  }
}
