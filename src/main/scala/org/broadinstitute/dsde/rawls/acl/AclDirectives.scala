package org.broadinstitute.dsde.rawls.acl

import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.routing.Directives._
import spray.routing.{AuthorizationFailedRejection, Directive0, Directive1}

class AclDirectives(workspaceService: WorkspaceService) {
  def getAcl(userId: String, workspaceNamespace: String, workspaceName: String): AccessLevel.Value = {
    workspaceService.getACLObject(userId, workspaceNamespace, workspaceName) match {
      case None => AccessLevel.None
      case Some(acl) => acl.maximumAccessLevel
    }
  }

  def withAccess(userId: String, workspaceNamespace: String, workspaceName: String): Directive1[AccessLevel.Value] = extract { requestContext =>
    getAcl(userId, workspaceNamespace, workspaceName)
  }

  def requireAccess(requiredLevel: AccessLevel.Value, userId: String, workspaceNamespace: String, workspaceName: String): Directive0 =
    withAccess(userId, workspaceNamespace, workspaceName) flatMap { userLevel =>
      if (userLevel >= requiredLevel)
        pass
      else
        reject(AuthorizationFailedRejection)
    }
}
