package org.broadinstitute.dsde.rawls.snapshot

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.workspace.model._
import cats.effect.{ContextShift, IO}
import com.google.cloud.bigquery.Acl
import com.google.cloud.bigquery.Acl.Entity
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleBigQueryServiceFactory, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.deltalayer.DeltaLayer
import org.broadinstitute.dsde.rawls.model.{ErrorReport, GoogleProjectId, NamedDataRepoSnapshot, SamPolicyWithNameAndEmail, SamResourceTypeNames, SamWorkspaceActions, SamWorkspacePolicyNames, UserInfo, WorkspaceAttributeSpecs, WorkspaceName}
import org.broadinstitute.dsde.rawls.util.{FutureSupport, WorkspaceSupport}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import spray.json.DefaultJsonProtocol._

import java.util.UUID
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object SnapshotService {

  def constructor(dataSource: SlickDataSource, samDAO: SamDAO, workspaceManagerDAO: WorkspaceManagerDAO, bqServiceFactory: GoogleBigQueryServiceFactory, terraDataRepoUrl: String, pathToCredentialJson: String, clientEmail: WorkbenchEmail, deltaLayerStreamerEmail: WorkbenchEmail)(userInfo: UserInfo)
                 (implicit executionContext: ExecutionContext, contextShift: ContextShift[IO]): SnapshotService = {
    new SnapshotService(userInfo, dataSource, samDAO, workspaceManagerDAO, bqServiceFactory, terraDataRepoUrl, pathToCredentialJson, clientEmail, deltaLayerStreamerEmail)
  }

}

class SnapshotService(protected val userInfo: UserInfo, val dataSource: SlickDataSource, val samDAO: SamDAO, workspaceManagerDAO: WorkspaceManagerDAO, bqServiceFactory: GoogleBigQueryServiceFactory, terraDataRepoInstanceName: String, pathToCredentialJson: String, clientEmail: WorkbenchEmail, deltaLayerStreamerEmail: WorkbenchEmail)
                     (implicit protected val executionContext: ExecutionContext, implicit val contextShift: ContextShift[IO])
  extends FutureSupport with WorkspaceSupport with LazyLogging {

  def createSnapshot(workspaceName: WorkspaceName, snapshot: NamedDataRepoSnapshot): Future[DataRepoSnapshotResource] = {
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write, Some(WorkspaceAttributeSpecs(all = false))).flatMap { workspaceContext =>
      if(!workspaceStubExists(workspaceContext.workspaceIdAsUUID, userInfo)) {
        workspaceManagerDAO.createWorkspace(workspaceContext.workspaceIdAsUUID, userInfo.accessToken)
      }

      val snapshotRef = workspaceManagerDAO.createDataRepoSnapshotReference(workspaceContext.workspaceIdAsUUID, snapshot.snapshotId, snapshot.name, snapshot.description, terraDataRepoInstanceName, CloningInstructionsEnum.NOTHING, userInfo.accessToken)

      val referenceId = snapshotRef.getMetadata.getResourceId
      val datasetName = DeltaLayer.generateDatasetName(referenceId)

      val datasetLabels = Map("workspace_id" -> workspaceContext.workspaceId, "snapshot_id" -> snapshot.snapshotId.toString)

      // create BQ dataset, get workspace policies from Sam, and add those Sam policies to the dataset IAM
      val createDatasetIO = for {
        samPolicies <- IO.fromFuture(IO(samDAO.listPoliciesForResource(SamResourceTypeNames.workspace, workspaceContext.workspaceId, userInfo)))
        aclBindings = calculateDatasetAcl(samPolicies)
        bqService = bqServiceFactory.getServiceFromCredentialPath(pathToCredentialJson, GoogleProject(workspaceName.namespace))
        _ <- bqService.use(_.createDataset(datasetName, datasetLabels, aclBindings))
      } yield { }

      createDatasetIO.unsafeToFuture().recover {
        case t: Throwable =>
          //fire and forget this undo, we've made our best effort to fix things at this point
          IO.pure(workspaceManagerDAO.deleteDataRepoSnapshotReference(workspaceContext.workspaceIdAsUUID, referenceId, userInfo.accessToken)).unsafeToFuture()
          throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.InternalServerError, s"Unable to create snapshot reference in workspace ${workspaceContext.workspaceId}. Error: ${t.getMessage}"))
      }.flatMap { _ =>

        val createBQReferenceFuture = for {
          petToken <- samDAO.getPetServiceAccountToken(GoogleProjectId(workspaceName.namespace), SamDAO.defaultScopes + SamDAO.bigQueryReadOnlyScope, userInfo)
          bigQueryRef = workspaceManagerDAO.createBigQueryDatasetReference(workspaceContext.workspaceIdAsUUID, new ReferenceResourceCommonFields().name(datasetName).cloningInstructions(CloningInstructionsEnum.NOTHING), new GcpBigQueryDatasetAttributes().projectId(workspaceContext.namespace).datasetId(datasetName), OAuth2BearerToken(petToken))
        } yield { bigQueryRef }

        createBQReferenceFuture.recover {
          case t: Throwable =>
            //fire and forget these undos, we've made our best effort to fix things at this point
            for {
              _ <- deleteBigQueryDataset(workspaceName, datasetName).unsafeToFuture()
              _ <- IO.pure(workspaceManagerDAO.deleteDataRepoSnapshotReference(workspaceContext.workspaceIdAsUUID, referenceId, userInfo.accessToken)).unsafeToFuture()
            } yield {}
            throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.InternalServerError, s"Unable to create snapshot reference in workspace ${workspaceContext.workspaceId}. Error: ${t.getMessage}"))
        }
      }.map { _ => snapshotRef}
    }
  }

  def getSnapshot(workspaceName: WorkspaceName, referenceId: String): Future[DataRepoSnapshotResource] = {
    val referenceUuid = validateSnapshotId(referenceId)
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))).flatMap { workspaceContext =>
      val ref = workspaceManagerDAO.getDataRepoSnapshotReference(workspaceContext.workspaceIdAsUUID, referenceUuid, userInfo.accessToken)
      Future.successful(ref)
    }
  }

  def massageSnapshots(references: ResourceList): Seq[DataRepoSnapshotResource] = {
    val res = new ListBuffer[DataRepoSnapshotResource]()
    references.getResources.forEach(r => { val massaged = new DataRepoSnapshotResource
                                          massaged.setAttributes(r.getResourceAttributes.getGcpDataRepoSnapshot)
                                          massaged.setMetadata(r.getMetadata)
                                          res += massaged
    } )
    res
  }

  def enumerateSnapshots(workspaceName: WorkspaceName, offset: Int, limit: Int): Future[Seq[DataRepoSnapshotResource]] = {
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))).map { workspaceContext =>
      Try(workspaceManagerDAO.enumerateDataRepoSnapshotReferences(workspaceContext.workspaceIdAsUUID, offset, limit, userInfo.accessToken)) match {
        case Success(references) => massageSnapshots(references)
        // if we fail with a 404, it means we have no stub in WSM yet. This is benign and functionally equivalent
        // to having no references, so return the empty list.
        case Failure(ex: bio.terra.workspace.client.ApiException) if ex.getCode == 404 => List()
        // but if we hit a different error, it's a valid error; rethrow it
        case Failure(ex: bio.terra.workspace.client.ApiException) =>
          throw new RawlsExceptionWithErrorReport(ErrorReport(ex.getCode, ex))
        case Failure(other) =>
          logger.warn(s"Unexpected error when enumerating snapshots: ${other.getMessage}")
          throw new RawlsExceptionWithErrorReport(ErrorReport(other))
      }
    }
  }

  def updateSnapshot(workspaceName: WorkspaceName, snapshotId: String, updateInfo: UpdateDataReferenceRequestBody): Future[Unit] = {
    val snapshotUuid = validateSnapshotId(snapshotId)
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write, Some(WorkspaceAttributeSpecs(all = false))).map { workspaceContext =>
      // check that snapshot exists before updating it. If the snapshot does not exist, the GET attempt will throw a 404
      // TODO: these WSM APIs are in the process of being deprecated. We should update with new APIs as they become available
      workspaceManagerDAO.getDataRepoSnapshotReference(workspaceContext.workspaceIdAsUUID, snapshotUuid, userInfo.accessToken)
      workspaceManagerDAO.updateDataRepoSnapshotReference(workspaceContext.workspaceIdAsUUID, snapshotUuid, updateInfo, userInfo.accessToken)
    }
  }

  def deleteSnapshot(workspaceName: WorkspaceName, snapshotId: String): Future[Unit] = {
    val snapshotUuid = validateSnapshotId(snapshotId)
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write, Some(WorkspaceAttributeSpecs(all = false))).map { workspaceContext =>
      // check that snapshot exists before deleting it. If the snapshot does not exist, the GET attempt will throw a 404
      val snapshotRef = workspaceManagerDAO.getDataRepoSnapshotReference(workspaceContext.workspaceIdAsUUID, snapshotUuid, userInfo.accessToken)
      workspaceManagerDAO.deleteDataRepoSnapshotReference(workspaceContext.workspaceIdAsUUID, snapshotUuid, userInfo.accessToken)

      val datasetName = DeltaLayer.generateDatasetName(snapshotRef.getMetadata.getResourceId)
      deleteBigQueryDataset(workspaceName, datasetName).unsafeToFuture().map { _ =>
        val datasetRef = workspaceManagerDAO.getBigQueryDatasetReferenceByName(workspaceContext.workspaceIdAsUUID, datasetName, userInfo.accessToken)
        workspaceManagerDAO.deleteBigQueryDatasetReference(workspaceContext.workspaceIdAsUUID, datasetRef.getMetadata.getResourceId, userInfo.accessToken)
      }.recover {
        case t: Throwable =>
          logger.warn(s"A snapshot reference was deleted, but an error occurred while deleting its Delta Layer companion dataset: snapshot ref ID: ${snapshotRef.getMetadata.getResourceId}, dataset name: ${datasetName}")
          throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.InternalServerError, s"Your snapshot reference was deleted, but an error occurred while deleting its Delta Layer companion dataset. Error: ${t.getMessage}"))
      }
    }
  }

  private def workspaceStubExists(workspaceId: UUID, userInfo: UserInfo): Boolean = {
    Try(workspaceManagerDAO.getWorkspace(workspaceId, userInfo.accessToken)).isSuccess
  }

  private def validateSnapshotId(snapshotId: String): UUID = {
    Try(UUID.fromString(snapshotId)) match {
      case Success(snapshotUuid) => snapshotUuid
      case Failure(_) =>
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "SnapshotId must be a valid UUID."))
    }
  }

  private def deleteBigQueryDataset(workspaceName: WorkspaceName, datasetName: String) = {
    val bqService = bqServiceFactory.getServiceFromCredentialPath(pathToCredentialJson, GoogleProject(workspaceName.namespace))
    bqService.use(_.deleteDataset(datasetName))
  }

  private def calculateDatasetAcl(samPolicies: Set[SamPolicyWithNameAndEmail]): Map[Acl.Role, Seq[(WorkbenchEmail, Entity.Type)]] = {

    val accessPolicies = Seq(SamWorkspacePolicyNames.owner, SamWorkspacePolicyNames.writer, SamWorkspacePolicyNames.reader)

    val defaultIamRoles = Map(Acl.Role.OWNER -> Seq((clientEmail, Acl.Entity.Type.USER)), Acl.Role.WRITER -> Seq((deltaLayerStreamerEmail, Acl.Entity.Type.USER)))

    val projectOwnerPolicy = samPolicies.filter(_.policyName == SamWorkspacePolicyNames.projectOwner).head.policy.memberEmails

    val filteredSamPolicies = samPolicies.filter(samPolicy => accessPolicies.contains(samPolicy.policyName)).map(_.email) ++ projectOwnerPolicy

    val samAclBindings = Acl.Role.READER -> filteredSamPolicies.map{ filteredSamPolicyEmail =>(filteredSamPolicyEmail, Acl.Entity.Type.GROUP) }.toSeq

    defaultIamRoles + samAclBindings
  }

}
