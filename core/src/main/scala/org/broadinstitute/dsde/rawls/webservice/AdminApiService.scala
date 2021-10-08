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

import java.util.UUID
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
            userServiceConstructor(userInfo).adminDeleteBillingProject(RawlsBillingProjectName(projectId), ownerInfo)
          }
        }
      }
    } ~
    path("admin" / "project" / "registration") {
      post {
        entity(as[RawlsBillingProjectTransfer]) { xfer =>
          complete { userServiceConstructor(userInfo).adminRegisterBillingProject(xfer) }
        }
      }
    } ~
    path("admin" / "project" / "registration" / Segment) { (projectName) =>
      delete {
        entity(as[Map[String, String]]) { ownerInfo =>
          complete { userServiceConstructor(userInfo).adminUnregisterBillingProjectWithOwnerInfo(RawlsBillingProjectName(projectName), ownerInfo) }
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
        complete { workspaceServiceConstructor(userInfo).adminAbortSubmission(WorkspaceName(workspaceNamespace, workspaceName), submissionId) }
      }
    } ~
    path("admin" / "submissions" / "queueStatusByUser") {
      get {
        complete { workspaceServiceConstructor(userInfo).adminWorkflowQueueStatusByUser }
      }
    } ~
    path("admin" / "user" / "role" / "curator" / Segment) { (userEmail) =>
      put {
        complete { userServiceConstructor(userInfo).adminAddLibraryCurator(RawlsUserEmail(userEmail)) }
      } ~
      delete {
        complete { userServiceConstructor(userInfo).adminRemoveLibraryCurator(RawlsUserEmail(userEmail)) }
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
    path("admin" / "workspaces" / "entities" / "cache") {
      get {
        complete { workspaceServiceConstructor(userInfo).adminListWorkspacesWithInvalidEntityCaches }
      } ~
      post {
        entity(as[Seq[String]]) { workspaceIds =>
          complete { workspaceServiceConstructor(userInfo).adminResetWorkspaceEntityCaches(workspaceIds) }
        }
      }
    } ~
    path("admin" / "refreshToken" / Segment ) { userSubjectId =>
      delete {
        complete { userServiceConstructor(userInfo).adminDeleteRefreshToken(RawlsUserRef(RawlsUserSubjectId(userSubjectId))) }
      }
    }
  }
}
