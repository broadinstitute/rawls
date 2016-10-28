package org.broadinstitute.dsde.rawls.webservice

/**
 * Created by tsharpe on 9/25/15.
 */

import java.net.URLDecoder

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.genomics.GenomicsService
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.statistics.StatisticsService
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.joda.time.DateTime
import spray.routing._

import scala.concurrent.ExecutionContext

trait AdminApiService extends HttpService with PerRequestCreator with UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._
  import spray.httpx.SprayJsonSupport._

  val workspaceServiceConstructor: UserInfo => WorkspaceService
  val userServiceConstructor: UserInfo => UserService
  val genomicsServiceConstructor: UserInfo => GenomicsService
  val statisticsServiceConstructor: UserInfo => StatisticsService

  val adminRoutes = requireUserInfo() { userInfo =>
    path("admin" / "billing" / "list" / Segment) { (userEmail) =>
      get {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.AdminListBillingProjectsForUser(RawlsUserEmail(userEmail)))
      }
    } ~
    path("admin" / "billing" / Segment) { (projectId) =>
      delete {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.AdminDeleteBillingProject(RawlsBillingProjectName(projectId)))
      }
    } ~
    path("admin" / "billing" / "register" / Segment) { (projectId) =>
      put {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.AdminRegisterBillingProject(RawlsBillingProjectName(projectId)))
      }
    } ~
    path("admin" / "billing" / "unregister" / Segment) { (projectId) =>
      delete {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.AdminUnregisterBillingProject(RawlsBillingProjectName(projectId)))
      }
    } ~
    path("admin" / "billing" / Segment / Segment / Segment) { (projectId, role, userEmail) =>
      put {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.AdminAddUserToBillingProject(RawlsBillingProjectName(projectId), ProjectAccessUpdate(userEmail, ProjectRoles.withName(role))))
      } ~
      delete {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.AdminRemoveUserFromBillingProject(RawlsBillingProjectName(projectId), ProjectAccessUpdate(userEmail, ProjectRoles.withName(role))))
      }
    } ~
    path("admin" / "submissions") {
      get {
        requestContext => perRequest(requestContext,
          WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.AdminListAllActiveSubmissions)
      }
    } ~
    path("admin" / "submissions" / Segment / Segment / Segment) { (workspaceNamespace, workspaceName, submissionId) =>
      delete {
        requestContext => perRequest(requestContext,
          WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.AdminAbortSubmission(WorkspaceName(workspaceNamespace, workspaceName), submissionId))
      }
    } ~
    path("admin" / "groups") { //create group
      post {
        entity(as[RawlsGroupRef]) { groupRef =>
          requestContext => perRequest(requestContext,
            UserService.props(userServiceConstructor, userInfo),
            UserService.AdminCreateGroup(groupRef))
        }
      } ~
      delete {
        entity(as[RawlsGroupRef]) { groupRef =>
          requestContext => perRequest(requestContext,
            UserService.props(userServiceConstructor, userInfo),
            UserService.AdminDeleteGroup(groupRef))
        }
      }
    } ~
    path("admin" / "groups" / Segment / "members") { (groupNameRaw) =>
      val groupName = URLDecoder.decode(groupNameRaw, "UTF-8")
      // there are 3 methods supported to modify group membership:
      // PUT = "set the group members to exactly this list"
      // POST = "add these things to the list"
      // DELETE = "remove these things from the list"
      put {
        entity(as[RawlsGroupMemberList]) { memberList =>
          requestContext => perRequest(requestContext,
            UserService.props(userServiceConstructor, userInfo),
            UserService.AdminOverwriteGroupMembers(RawlsGroupRef(RawlsGroupName(groupName)), memberList))
        }
      } ~
      post {
        entity(as[RawlsGroupMemberList]) { memberList =>
          requestContext => perRequest(requestContext,
            UserService.props(userServiceConstructor, userInfo),
            UserService.AdminAddGroupMembers(RawlsGroupRef(RawlsGroupName(groupName)), memberList))
        }
      } ~
      delete {
        entity(as[RawlsGroupMemberList]) { memberList =>
          requestContext => perRequest(requestContext,
            UserService.props(userServiceConstructor, userInfo),
            UserService.AdminRemoveGroupMembers(RawlsGroupRef(RawlsGroupName(groupName)), memberList))
        }
      } ~
      get {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.AdminListGroupMembers(groupName))
      }
    } ~
    path("admin" / "groups" / Segment / "sync") { (groupNameRaw) =>
      val groupName = URLDecoder.decode(groupNameRaw, "UTF-8")
      post {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.AdminSynchronizeGroupMembers(RawlsGroupRef(RawlsGroupName(groupName))))
      }
    } ~
    path("admin" / "users") {
      get {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.AdminListUsers)
      } ~
      post {
        entity(as[RawlsUserInfoList]) { userInfoList =>
          requestContext => perRequest(requestContext,
            UserService.props(userServiceConstructor, userInfo),
            UserService.AdminImportUsers(userInfoList))
        }
      }
    } ~
    path("admin" / "user" / "role" / "curator" / Segment) { (userEmail) =>
      put {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.AdminAddLibraryCurator(RawlsUserEmail(userEmail)))
      } ~
      delete {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.AdminRemoveLibraryCurator(RawlsUserEmail(userEmail)))
      }
    } ~
    path("admin" / "user" / Segment) { userSubjectId =>
      get {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.AdminGetUserStatus(RawlsUserRef(RawlsUserSubjectId(userSubjectId))))
      } ~
      delete {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.AdminDeleteUser(RawlsUserRef(RawlsUserSubjectId(userSubjectId))))
      }
    } ~
    path("admin" / "user" / "ldap" / Segment) { userSubjectId =>
      put {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.AdminAddToLDAP(RawlsUserSubjectId(userSubjectId)))
      } ~
      delete {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.AdminRemoveFromLDAP(RawlsUserSubjectId(userSubjectId)))
      }
    } ~
    path("admin"/ "user" / Segment / "enable") { userSubjectId =>
      post {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.AdminEnableUser(RawlsUserRef(RawlsUserSubjectId(userSubjectId))))
      }
    } ~
    path("admin"/ "user" / Segment / "disable") { userSubjectId =>
      post {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.AdminDisableUser(RawlsUserRef(RawlsUserSubjectId(userSubjectId))))
      }
    } ~
    path("admin" / "allUserReadAccess" / Segment / Segment) { (workspaceNamespace, workspaceName) =>
      get {
        requestContext => perRequest(requestContext,
          WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.HasAllUserReadAccess(WorkspaceName(workspaceNamespace, workspaceName)))
      } ~
      put {
        requestContext => perRequest(requestContext,
          WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.GrantAllUserReadAccess(WorkspaceName(workspaceNamespace, workspaceName)))
      } ~
      delete {
        requestContext => perRequest(requestContext,
          WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.RevokeAllUserReadAccess(WorkspaceName(workspaceNamespace, workspaceName)))
      }
    } ~
    path("admin" / "validate" / Segment / Segment) { (workspaceNamespace, workspaceName) =>
      get {
        parameters('userSubjectId.?) { (userSubjectId) =>
          requestContext => perRequest(requestContext,
            WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.GetWorkspaceStatus(WorkspaceName(workspaceNamespace, workspaceName), userSubjectId))
        }
      }
    } ~
    path("admin" / "workspaces") {
      get {
        parameters('attributeName.?, 'valueString.?, 'valueNumber.?, 'valueBoolean.?) { (nameOption, stringOption, numberOption, booleanOption) =>
          requestContext =>
            val msg = nameOption match {
              case None => WorkspaceService.ListAllWorkspaces
              case Some(attributeName) =>
                val name = AttributeName.fromDelimitedName(attributeName)
                (stringOption, numberOption, booleanOption) match {
                  case (Some(string), None, None) => WorkspaceService.AdminListWorkspacesWithAttribute(name, AttributeString(string))
                  case (None, Some(number), None) => WorkspaceService.AdminListWorkspacesWithAttribute(name, AttributeNumber(number.toDouble))
                  case (None, None, Some(boolean)) => WorkspaceService.AdminListWorkspacesWithAttribute(name, AttributeBoolean(boolean.toBoolean))
                  case _ => throw new RawlsException("Specify exactly one of valueString, valueNumber, or valueBoolean")
                }
            }
            perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo), msg)
        }
      }
    } ~
    path("admin" / "workspaces" / Segment / Segment ) { (workspaceNamespace, workspaceName) =>
      delete {
        requestContext => perRequest(requestContext,
          WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.AdminDeleteWorkspace(WorkspaceName(workspaceNamespace, workspaceName))
        )
      }
    } ~
    path("admin" / "refreshToken" / Segment ) { userSubjectId =>
      delete {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.AdminDeleteRefreshToken(RawlsUserRef(RawlsUserSubjectId(userSubjectId)))
        )
      }
    } ~
    path("admin" / "allRefreshTokens" ) {
      delete {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.AdminDeleteAllRefreshTokens
        )
      }
    } ~
    path("admin" / "genomics" / "operations" / Segment ) { jobId =>
      get {
        requestContext => perRequest(requestContext,
          GenomicsService.props(genomicsServiceConstructor, userInfo),
          GenomicsService.GetOperation(jobId)
        )
      }
    } ~
    path("admin" / "statistics") {
      get {
        parameters('startDate, 'endDate) { (startDate, endDate) =>
          requestContext => perRequest(requestContext,
            StatisticsService.props(statisticsServiceConstructor, userInfo),
            StatisticsService.GetStatistics(startDate, endDate)
          )
        }
      }
    }
  }
}
