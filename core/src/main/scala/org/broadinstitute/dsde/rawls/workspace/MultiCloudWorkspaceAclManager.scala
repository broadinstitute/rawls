package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.model.StatusCodes
import bio.terra.workspace.model.{IamRole, RoleBinding, RoleBindingList}
import cats.implicits.catsSyntaxOptionId
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.billing.BillingProfileManagerDAO
import org.broadinstitute.dsde.rawls.billing.BillingProfileManagerDAO.ProfilePolicy
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{
  AccessEntry,
  ErrorReport,
  RawlsBillingProjectName,
  RawlsRequestContext,
  SamResourcePolicyName,
  SamWorkspacePolicyNames,
  Workspace,
  WorkspaceACL,
  WorkspaceAccessLevels,
  WorkspaceName
}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

class MultiCloudWorkspaceAclManager(workspaceManagerDAO: WorkspaceManagerDAO,
                                    val samDAO: SamDAO,
                                    billingProfileManagerDAO: BillingProfileManagerDAO,
                                    dataSource: SlickDataSource
)(implicit
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
    Option(IamRole.fromValue(policyName.value.toUpperCase)) match {
      case Some(role) => workspaceManagerDAO.grantRole(workspace.workspaceIdAsUUID, email, role, ctx)
      case None =>
        throw new InvalidWorkspaceAclUpdateException(
          ErrorReport(StatusCodes.InternalServerError, s"unsupported policy $policyName")
        )
    }
  )

  def removeUserFromPolicy(workspace: Workspace,
                           policyName: SamResourcePolicyName,
                           email: WorkbenchEmail,
                           ctx: RawlsRequestContext
  ): Future[Unit] = Future(
    Option(IamRole.fromValue(policyName.value.toUpperCase)) match {
      case Some(role) => workspaceManagerDAO.removeRole(workspace.workspaceIdAsUUID, email, role, ctx)
      case None =>
        throw new InvalidWorkspaceAclUpdateException(
          ErrorReport(StatusCodes.InternalServerError, s"unsupported policy $policyName")
        )
    }
  )

  def maybeShareWorkspaceNamespaceCompute(
    policyAdditions: Set[(SamResourcePolicyName, String)],
    workspaceName: WorkspaceName,
    ctx: RawlsRequestContext
  ): Future[Unit] = {
    val newWriterEmails = policyAdditions.collect { case (SamWorkspacePolicyNames.writer, email) => email }

    if (newWriterEmails.nonEmpty) {
      for {
        workspaceBillingProject <- dataSource.inTransaction(
          _.rawlsBillingProjectQuery.load(RawlsBillingProjectName(workspaceName.namespace))
        )
        workspaceBillingProfileId = workspaceBillingProject
          .getOrElse(
            throw new RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.InternalServerError,
                          s"workspace ${workspaceName.toString} billing project does not exist"
              )
            )
          )
          .billingProfileId
          .getOrElse(
            throw new RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.InternalServerError,
                          s"workspace ${workspaceName.toString} billing profile does not exist"
              )
            )
          )
        _ <- Future
          .traverse(newWriterEmails) { email =>
            Future(
              billingProfileManagerDAO
                .addProfilePolicyMember(UUID.fromString(workspaceBillingProfileId),
                                        ProfilePolicy.PetCreator,
                                        email,
                                        ctx
                )
            )
          }
      } yield ()
    } else Future.successful()
  }
}
