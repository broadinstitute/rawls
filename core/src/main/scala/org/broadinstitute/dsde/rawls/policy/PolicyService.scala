package org.broadinstitute.dsde.rawls.policy

import bio.terra.policy.client.ApiException
import bio.terra.policy.model.{TpsComponent, TpsObjectType, TpsPaoCreateRequest, TpsPaoGetResult, TpsPaoSourceRequest, TpsPolicyInput, TpsPolicyInputs, TpsPolicyPair, TpsUpdateMode}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.tps.TpsDAO
import org.broadinstitute.dsde.rawls.model.TpsModel.{TERRA_POLICY_NAMESPACE, TpsPolicies}
import org.broadinstitute.dsde.rawls.model.{ErrorReport, ManagedGroupRef, RawlsGroupName, RawlsRequestContext, WorkspaceRequest}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

class PolicyService(tpsDAO: TpsDAO)(implicit val ec: ExecutionContext) extends LazyLogging {
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

  def getPao(objectId: UUID, ctx: RawlsRequestContext): Future[Option[TpsPaoGetResult]] =
    tpsDAO.getPao(objectId, ctx).map(Option.apply).recover {
      case ex: ApiException if ex.getCode == 404 =>
        None
    }

  /**
    * Retrieves the snapshot PAO for the given snapshotId. If it does not exist, it creates a new one with no policies.
    */
  def getOrCreateSnapshotPao(snapshotId: UUID, ctx: RawlsRequestContext): Future[TpsPaoGetResult] = {
    for {
      snapshotPaoOpt <- getPao(snapshotId, ctx)
      snapshotPao <- if (snapshotPaoOpt.isEmpty) {
        logger.info(s"pao not found, creating new one [snapshotId: $snapshotId]")
        val req = new TpsPaoCreateRequest()
          .objectType(TpsObjectType.SNAPSHOT)
          .objectId(snapshotId)
          .component(TpsComponent.TDR)

        tpsDAO.createPao(req, ctx).flatMap(_ => tpsDAO.getPao(snapshotId, ctx))
      } else {
        Future.successful(snapshotPaoOpt.get)
      }
    } yield snapshotPao
  }


  def deleteWorkspacePao(workspaceId: UUID, ctx: RawlsRequestContext): Future[Unit] =
    tpsDAO.deletePao(workspaceId, ctx).recover { case ex: ApiException =>
      logger.error(s"Exception occurred while deleting PAO: ${ex.getMessage}", ex)
    // Ignore any exception
    }

  def linkSnapshotPaoToWorkspacePao(
    snapshotId: UUID,
    workspaceId: UUID,
    dryRun: Boolean,
    ctx: RawlsRequestContext
  ): Future[Unit] = {
    val req = new TpsPaoSourceRequest().sourceObjectId(snapshotId)

    if (dryRun)
      req.setUpdateMode(TpsUpdateMode.DRY_RUN)
    else
      req.setUpdateMode(TpsUpdateMode.FAIL_ON_CONFLICT)

    tpsDAO.linkPao(req, workspaceId, ctx).map { res =>
      if (res.getConflicts.asScala.nonEmpty) {
        throw new RawlsExceptionWithErrorReport(ErrorReport(s"${if (dryRun) "Dry run" else "Link"} failed: ${res.getConflicts.asScala.mkString(",")}"))
      }
    }
  }
}
