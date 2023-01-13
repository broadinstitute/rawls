package org.broadinstitute.dsde.rawls.workspace

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.model.{
  AccessEntry,
  RawlsRequestContext,
  SamBillingProjectPolicyNames,
  SamPolicyWithNameAndEmail,
  SamResourcePolicyName,
  SamResourceTypeNames,
  SamWorkspacePolicyNames,
  Workspace,
  WorkspaceACL,
  WorkspaceAccessLevels,
  WorkspaceName
}
import org.broadinstitute.dsde.workbench.model.{WorkbenchEmail, WorkbenchException}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class RawlsWorkspaceAclManager(val samDAO: SamDAO)(implicit val executionContext: ExecutionContext)
    extends WorkspaceAclManager
    with LazyLogging {
  def getWorkspacePolicies(workspaceId: UUID,
                           ctx: RawlsRequestContext
  ): Future[Set[(WorkbenchEmail, SamResourcePolicyName)]] = for {
    existingPolicies <- samDAO.listPoliciesForResource(SamResourceTypeNames.workspace, workspaceId.toString, ctx)

    // the acl update code does not deal with the can catalog permission, there are separate functions for that.
    // exclude any existing can catalog policies so we don't inadvertently remove them
    existingPoliciesExcludingCatalog =
      existingPolicies.filterNot(_.policyName == SamWorkspacePolicyNames.canCatalog)
  } yield existingPoliciesExcludingCatalog.flatMap(p => p.policy.memberEmails.map(email => email -> p.policyName))

  def getAcl(workspaceId: UUID, ctx: RawlsRequestContext): Future[WorkspaceACL] = {

    def loadPolicy(policyName: SamResourcePolicyName,
                   policyList: Set[SamPolicyWithNameAndEmail]
    ): SamPolicyWithNameAndEmail =
      policyList
        .find(_.policyName.value.equalsIgnoreCase(policyName.value))
        .getOrElse(throw new WorkbenchException(s"Could not load $policyName policy"))

    val policyMembers =
      samDAO.listPoliciesForResource(SamResourceTypeNames.workspace, workspaceId.toString, ctx).map { currentACL =>
        val ownerPolicyMembers = loadPolicy(SamWorkspacePolicyNames.owner, currentACL).policy.memberEmails
        val writerPolicyMembers = loadPolicy(SamWorkspacePolicyNames.writer, currentACL).policy.memberEmails
        val readerPolicyMembers = loadPolicy(SamWorkspacePolicyNames.reader, currentACL).policy.memberEmails
        val shareReaderPolicyMembers = loadPolicy(SamWorkspacePolicyNames.shareReader, currentACL).policy.memberEmails
        val shareWriterPolicyMembers = loadPolicy(SamWorkspacePolicyNames.shareWriter, currentACL).policy.memberEmails
        val computePolicyMembers = loadPolicy(SamWorkspacePolicyNames.canCompute, currentACL).policy.memberEmails
        // note: can-catalog is a policy on the side and is not a part of the core workspace ACL so we won't load it

        (ownerPolicyMembers,
         writerPolicyMembers,
         readerPolicyMembers,
         shareReaderPolicyMembers,
         shareWriterPolicyMembers,
         computePolicyMembers
        )
      }

    policyMembers.flatMap {
      case (ownerPolicyMembers,
            writerPolicyMembers,
            readerPolicyMembers,
            shareReaderPolicyMembers,
            shareWriterPolicyMembers,
            computePolicyMembers
          ) =>
        val sharers = shareReaderPolicyMembers ++ shareWriterPolicyMembers

        for {
          ownersPending <- Future.traverse(ownerPolicyMembers) { email =>
            isUserPending(email.value, ctx).map(pending => email -> pending)
          }
          writersPending <- Future.traverse(writerPolicyMembers) { email =>
            isUserPending(email.value, ctx).map(pending => email -> pending)
          }
          readersPending <- Future.traverse(readerPolicyMembers) { email =>
            isUserPending(email.value, ctx).map(pending => email -> pending)
          }
        } yield {
          val owners = ownerPolicyMembers.map(email =>
            email.value -> AccessEntry(WorkspaceAccessLevels.Owner,
                                       ownersPending.toMap.getOrElse(email, true),
                                       true,
                                       true
            )
          ) // API_CHANGE: pending owners used to show as false for canShare and canCompute. they now show true. this is more accurate anyway
          val writers = writerPolicyMembers.map(email =>
            email.value -> AccessEntry(WorkspaceAccessLevels.Write,
                                       writersPending.toMap.getOrElse(email, true),
                                       sharers.contains(email),
                                       computePolicyMembers.contains(email)
            )
          )
          val readers = readerPolicyMembers.map(email =>
            email.value -> AccessEntry(WorkspaceAccessLevels.Read,
                                       readersPending.toMap.getOrElse(email, true),
                                       sharers.contains(email),
                                       computePolicyMembers.contains(email)
            )
          )

          WorkspaceACL((owners ++ writers ++ readers).toMap)
        }
    }
  }

  def addUserToPolicy(workspace: Workspace,
                      policyName: SamResourcePolicyName,
                      email: WorkbenchEmail,
                      ctx: RawlsRequestContext
  ): Future[Unit] =
    samDAO.addUserToPolicy(SamResourceTypeNames.workspace, workspace.workspaceId, policyName, email.value, ctx)

  def removeUserFromPolicy(workspace: Workspace,
                           policyName: SamResourcePolicyName,
                           email: WorkbenchEmail,
                           ctx: RawlsRequestContext
  ): Future[Unit] =
    samDAO.removeUserFromPolicy(SamResourceTypeNames.workspace, workspace.workspaceId, policyName, email.value, ctx)

  def maybeShareWorkspaceNamespaceCompute(policyAdditions: Set[(SamResourcePolicyName, String)],
                                          workspaceName: WorkspaceName,
                                          ctx: RawlsRequestContext
  ): Future[Unit] = {
    val newWriterEmails = policyAdditions.collect { case (SamWorkspacePolicyNames.canCompute, email) =>
      email
    }
    Future
      .traverse(newWriterEmails) { email =>
        samDAO
          .addUserToPolicy(SamResourceTypeNames.billingProject,
                           workspaceName.namespace,
                           SamBillingProjectPolicyNames.canComputeUser,
                           email,
                           ctx
          )
          .recoverWith { case regrets: Throwable =>
            logger.info(
              s"error adding user to canComputeUser policy of Terra billing project while updating ${workspaceName.toString} likely because it is a v2 billing project which does not have a canComputeUser policy. regrets: ${regrets.getMessage}"
            )
            Future.successful(())
          }
      }
      .map(_ => ())
  }
}
