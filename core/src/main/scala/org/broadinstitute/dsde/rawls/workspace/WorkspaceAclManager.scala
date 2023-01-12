package org.broadinstitute.dsde.rawls.workspace

import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.model.{
  RawlsRequestContext,
  SamResourcePolicyName,
  Workspace,
  WorkspaceACL,
  WorkspaceName
}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

trait WorkspaceAclManager {
  val samDAO: SamDAO
  implicit val executionContext: ExecutionContext

  def getWorkspacePolicies(workspaceId: UUID,
                           ctx: RawlsRequestContext
  ): Future[Set[(WorkbenchEmail, SamResourcePolicyName)]]

  def getAcl(workspaceId: UUID, ctx: RawlsRequestContext): Future[WorkspaceACL]

  def addUserToPolicy(workspace: Workspace,
                      policyName: SamResourcePolicyName,
                      email: WorkbenchEmail,
                      ctx: RawlsRequestContext
  ): Future[Unit]

  def removeUserFromPolicy(workspace: Workspace,
                           policyName: SamResourcePolicyName,
                           email: WorkbenchEmail,
                           ctx: RawlsRequestContext
  ): Future[Unit]

  def maybeShareWorkspaceNamespaceCompute(
    policyAdditions: Set[(SamResourcePolicyName, String)],
    workspaceName: WorkspaceName,
    ctx: RawlsRequestContext
  ): Future[Unit]

  def isUserPending(userEmail: String, ctx: RawlsRequestContext): Future[Boolean] =
    samDAO.getUserIdInfo(userEmail, ctx).map {
      case SamDAO.User(x)  => x.googleSubjectId.isEmpty
      case SamDAO.NotUser  => false
      case SamDAO.NotFound => true
    }
}
