package org.broadinstitute.dsde.rawls.workspace

import bio.terra.workspace.model.{IamRole, RoleBinding, RoleBindingList}
import cats.implicits.catsSyntaxOptionId
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{
  AccessEntry,
  RawlsRequestContext,
  SamResourcePolicyName,
  SamWorkspacePolicyNames,
  Workspace,
  WorkspaceACL,
  WorkspaceAccessLevels
}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

class MultiCloudWorkspaceAclManager(workspaceManagerDAO: WorkspaceManagerDAO, val samDAO: SamDAO)(implicit
  val executionContext: ExecutionContext
) extends WorkspaceAclManager {
  def getWorkspacePolicies(workspaceId: UUID,
                           ctx: RawlsRequestContext
  ): Future[Set[(WorkbenchEmail, SamResourcePolicyName)]] = {
    def roleBindingsToUserPolicies(roleBindingList: RoleBindingList): Set[(WorkbenchEmail, SamResourcePolicyName)] =
      roleBindingList.asScala.toSet
        .flatMap { roleBinding: RoleBinding =>
          roleBinding.getMembers.asScala.toSet.map { member: String =>
            (WorkbenchEmail(member), wsmIamRoleToSamPolicyName(roleBinding.getRole))
          }
        }
        .collect { case (email, Some(iamRole)) => (email, iamRole) }

    def wsmIamRoleToSamPolicyName(iamRole: IamRole): Option[SamResourcePolicyName] =
      iamRole match {
        case IamRole.OWNER  => SamWorkspacePolicyNames.owner.some
        case IamRole.WRITER => SamWorkspacePolicyNames.writer.some
        case IamRole.READER => SamWorkspacePolicyNames.reader.some
        case _              => None
      }

    for {
      roleBindings <- Future(workspaceManagerDAO.getRoles(workspaceId, ctx))
    } yield roleBindingsToUserPolicies(roleBindings)
  }

  def getAcl(workspaceId: UUID, ctx: RawlsRequestContext): Future[WorkspaceACL] = {
    def roleBindingToAccessEntryList(roleBinding: RoleBinding): Future[List[(String, AccessEntry)]] =
      WorkspaceAccessLevels.withPolicyName(roleBinding.getRole.getValue) match {
        case Some(workspaceAccessLevel) =>
          Future.traverse(roleBinding.getMembers.asScala.toList) { email =>
            isUserPending(email, ctx).map { pending =>
              email -> AccessEntry(workspaceAccessLevel,
                                   pending,
                                   workspaceAccessLevel.equals(WorkspaceAccessLevels.Owner),
                                   workspaceAccessLevel.equals(WorkspaceAccessLevels.Owner)
              )
            }
          }
        case None => Future.successful(List.empty)
      }

    for {
      roleBindings <- Future(
        workspaceManagerDAO.getRoles(workspaceId, ctx)
      )
      workspaceACL <- Future.traverse(roleBindings.asScala.toList) { roleBinding =>
        roleBindingToAccessEntryList(roleBinding)
      }
    } yield WorkspaceACL(workspaceACL.flatten.toMap)
  }

  def addUserToPolicy(workspace: Workspace,
                      policyName: SamResourcePolicyName,
                      email: WorkbenchEmail,
                      ctx: RawlsRequestContext
  ): Future[Unit] = Future(
    Option(IamRole.fromValue(policyName.value.toUpperCase)).map { role =>
      workspaceManagerDAO.grantRole(workspace.workspaceIdAsUUID, email, role, ctx)
    }
  )

  def removeUserFromPolicy(workspace: Workspace,
                           policyName: SamResourcePolicyName,
                           email: WorkbenchEmail,
                           ctx: RawlsRequestContext
  ): Future[Unit] = Future(
    Option(IamRole.fromValue(policyName.value.toUpperCase)).map { role =>
      workspaceManagerDAO.removeRole(workspace.workspaceIdAsUUID, email, role, ctx)
    }
  )
}
