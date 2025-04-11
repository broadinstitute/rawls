package org.broadinstitute.dsde.rawls.policy

import bio.terra.policy.model.{
  TpsComponent,
  TpsObjectType,
  TpsPaoCreateRequest,
  TpsPaoSourceRequest,
  TpsPolicyInput,
  TpsPolicyInputs,
  TpsPolicyPair,
  TpsUpdateMode
}
import org.broadinstitute.dsde.rawls.dataaccess.tps.TpsDAO
import org.broadinstitute.dsde.rawls.model.TpsModel.{TERRA_POLICY_NAMESPACE, TpsPolicies}
import org.broadinstitute.dsde.rawls.model.{ManagedGroupRef, RawlsGroupName, RawlsRequestContext, WorkspaceRequest}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

class PolicyService(tpsDAO: TpsDAO)(implicit val ec: ExecutionContext) {
  def createWorkspacePao(workspaceId: UUID,
                         workspaceRequest: WorkspaceRequest,
                         ctx: RawlsRequestContext
  ): Future[Unit] = {
    val req =
      new TpsPaoCreateRequest()
        .objectType(TpsObjectType.WORKSPACE)
        .objectId(workspaceId)
        .component(TpsComponent.RAWLS)
    val protectedDataPolicy =
      new TpsPolicyInput().namespace(TERRA_POLICY_NAMESPACE).name(TpsPolicies.ProtectedData.name)

    (workspaceRequest.authorizationDomain, workspaceRequest.enhancedBucketLogging) match {
      case (Some(authDomain), _) if authDomain.nonEmpty =>
        val authDomainGroups = authDomain.map { case ManagedGroupRef(RawlsGroupName(membersGroupName)) =>
          new TpsPolicyPair().key(TpsPolicies.GroupConstraint.additionalDataKey).value(membersGroupName)
        }
        val groupConstraintPolicy = new TpsPolicyInput()
          .namespace(TERRA_POLICY_NAMESPACE)
          .name(TpsPolicies.GroupConstraint.name)
          .additionalData(authDomainGroups.toList.asJava)
        req.setAttributes(new TpsPolicyInputs().inputs(List(protectedDataPolicy, groupConstraintPolicy).asJava))
      case (None, Some(enhancedBucketLogging)) if enhancedBucketLogging =>
        req.setAttributes(new TpsPolicyInputs().inputs(List(protectedDataPolicy).asJava))
      case _ =>
    }

    tpsDAO.createPao(req, ctx)
  }

  /**
    * The documentation for this TPS API is misleading. It will merge the target PAO into the source
    * PAO, NOT the other way around. While it might be confusing to see the destination workspace id
    * used as the `sourceObjectId`, this is the correct way to call the TPS API.
    */
  def mergeWorkspacePao(sourceWorkspaceId: UUID, destWorkspaceId: UUID, ctx: RawlsRequestContext): Future[Unit] = {
    val req = new TpsPaoSourceRequest().sourceObjectId(destWorkspaceId).updateMode(TpsUpdateMode.FAIL_ON_CONFLICT)

    tpsDAO.mergePao(req, sourceWorkspaceId, ctx)
  }

}
