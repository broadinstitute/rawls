package org.broadinstitute.dsde.rawls.snapshot

import akka.http.scaladsl.model.StatusCodes
import bio.terra.workspace.model._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.deltalayer.DeltaLayer
import org.broadinstitute.dsde.rawls.model.{DataReferenceName, ErrorReport, NamedDataRepoSnapshot, SamWorkspaceActions, SnapshotListResponse, UserInfo, WorkspaceAttributeSpecs, WorkspaceName}
import org.broadinstitute.dsde.rawls.util.{FutureSupport, WorkspaceSupport}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

import java.util.UUID
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object SnapshotService {

  def constructor(dataSource: SlickDataSource, samDAO: SamDAO, workspaceManagerDAO: WorkspaceManagerDAO, deltaLayer: DeltaLayer, terraDataRepoUrl: String, clientEmail: WorkbenchEmail, deltaLayerStreamerEmail: WorkbenchEmail)(userInfo: UserInfo)
                 (implicit executionContext: ExecutionContext): SnapshotService = {
    new SnapshotService(userInfo, dataSource, samDAO, workspaceManagerDAO, deltaLayer, terraDataRepoUrl, clientEmail, deltaLayerStreamerEmail)
  }

}

class SnapshotService(protected val userInfo: UserInfo, val dataSource: SlickDataSource, val samDAO: SamDAO, workspaceManagerDAO: WorkspaceManagerDAO, deltaLayer: DeltaLayer, terraDataRepoInstanceName: String, clientEmail: WorkbenchEmail, deltaLayerStreamerEmail: WorkbenchEmail)
                     (implicit protected val executionContext: ExecutionContext)
  extends FutureSupport with WorkspaceSupport with LazyLogging {

  def createSnapshot(workspaceName: WorkspaceName, snapshot: NamedDataRepoSnapshot): Future[DataRepoSnapshotResource] = {
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write, Some(WorkspaceAttributeSpecs(all = false))).flatMap { workspaceContext =>
      val wsid = workspaceContext.workspaceIdAsUUID // to avoid UUID parsing multiple times
      // create the stub workspace in WSM if it does not already exist
      if(!workspaceStubExists(wsid, userInfo)) {
        workspaceManagerDAO.createWorkspace(wsid, userInfo.accessToken)
      }
      // create the requested snapshot reference
      val snapshotRef = workspaceManagerDAO.createDataRepoSnapshotReference(wsid, snapshot.snapshotId, snapshot.name,
        snapshot.description, terraDataRepoInstanceName, CloningInstructionsEnum.NOTHING, userInfo.accessToken)

      // attempt to create the BQ dataset, which might already exist
      deltaLayer.createDatasetIfNotExist(workspaceContext, userInfo).recover {
        case t: Throwable =>
          // something went wrong creating the companion dataset
          logger.warn(s"Error creating Delta Layer companion dataset for workspace $workspaceName: ${t.getMessage}")
          // since companion dataset creation failed, try to clean up this snapshot reference so the user can try again
          Try(workspaceManagerDAO.deleteDataRepoSnapshotReference(wsid, snapshotRef.getMetadata.getResourceId, userInfo.accessToken)) match {
            case Success(_) =>
              throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.InternalServerError,
                s"Unable to create snapshot reference in workspace ${workspaceContext.workspaceId} due to problems creating " +
                  s"the Delta Layer companion dataset. Error: [${t.getMessage}]"))
            case Failure(ex) =>
              throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.InternalServerError,
                s"Error while creating snapshot reference in workspace ${workspaceContext.workspaceId}. Additionally, there " +
                  s"was an error cleaning up the snapshot reference. The reference may be in an unusable state. Original error " +
                  s"during Delta Layer companion dataset creation was: [${t.getMessage}]. " +
                  s"Error during snapshot reference cleanup: [${ex.getMessage}]."))
          }
      }.map { _ =>
        snapshotRef
      }
    }
  }

  def getSnapshot(workspaceName: WorkspaceName, referenceId: String): Future[DataRepoSnapshotResource] = {
    val referenceUuid = validateSnapshotId(referenceId)
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))).flatMap { workspaceContext =>
      val ref = workspaceManagerDAO.getDataRepoSnapshotReference(workspaceContext.workspaceIdAsUUID, referenceUuid, userInfo.accessToken)
      Future.successful(ref)
    }
  }

  def getSnapshotByName(workspaceName: WorkspaceName, referenceName: String): Future[DataRepoSnapshotResource] = {
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))).flatMap { workspaceContext =>
      val ref = workspaceManagerDAO.getDataRepoSnapshotReferenceByName(workspaceContext.workspaceIdAsUUID, DataReferenceName(referenceName), userInfo.accessToken)
      Future.successful(ref)
    }
  }

  //AS-787 - rework the data so that it's in the same place in the JSON with a list and get snapshot responses
  def massageSnapshots(references: ResourceList): SnapshotListResponse = {
    val snapshots = references.getResources.asScala.map { r =>
      val massaged = new DataRepoSnapshotResource
      massaged.setAttributes(r.getResourceAttributes.getGcpDataRepoSnapshot)
      massaged.setMetadata(r.getMetadata)
      massaged
    }
    SnapshotListResponse(snapshots)
  }

  /*
    internal method to query WSM for a list of snapshot references; used by enumerateSnapshots and findBySnapshotId
   */
  protected[snapshot] def retrieveSnapshotReferences(workspaceId: UUID, offset: Int, limit: Int): SnapshotListResponse = {
    Try(workspaceManagerDAO.enumerateDataRepoSnapshotReferences(workspaceId, offset, limit, userInfo.accessToken)) match {
      case Success(references) => massageSnapshots(references)
      // if we fail with a 404, it means we have no stub in WSM yet. This is benign and functionally equivalent
      // to having no references, so return the empty list.
      case Failure(ex: bio.terra.workspace.client.ApiException) if ex.getCode == 404 => new SnapshotListResponse(Seq.empty[DataRepoSnapshotResource])
      // but if we hit a different error, it's a valid error; rethrow it
      case Failure(ex: bio.terra.workspace.client.ApiException) =>
        throw new RawlsExceptionWithErrorReport(ErrorReport(ex.getCode, ex))
      case Failure(other) =>
        logger.warn(s"Unexpected error when enumerating snapshots: ${other.getMessage}")
        throw new RawlsExceptionWithErrorReport(ErrorReport(other))
    }
  }

  /**
    * return a given page of snapshot references from Workspace Manager, optionally returning only those
    * snapshot references that refer to a supplied TDR snapshotId.
    *
    * @param workspaceName the workspace owning the snapshot references
    * @param offset pagination offset for the list
    * @param limit pagination limit for the list
    * @param referencedSnapshotId the TDR snapshotId for which to return matching references
    * @return the list of snapshot references
    */
  def enumerateSnapshots(workspaceName: WorkspaceName, offset: Int, limit: Int, referencedSnapshotId: Option[UUID] = None): Future[SnapshotListResponse] = {
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))).map { workspaceContext =>
      referencedSnapshotId match {
        case None => retrieveSnapshotReferences(workspaceContext.workspaceIdAsUUID, offset, limit)
        case Some(id) => findBySnapshotId(workspaceContext.workspaceIdAsUUID, id, offset, limit)
      }
    }
  }

   /*
    * Returns all snapshot references from Workspace Manager that refer to a specified snapshotId.
    * Internal method; does not check authorization for the given workspace.
    *
    * @param workspaceName the workspace owning the snapshot references
    * @param snapshotId the snapshotId to look for
    * @param userOffset pagination offset for the final list of results
    * @param userLimit pagination limit for the final list of results
    * @param batchSize optional, default 200: internal param exposed for unit-testing purposes that controls
    *                  the size of pages requested from Workspace Manager while looking for the specified
    *                  snapshotId.
    * @return the list of all snapshot references that refer to the specified snapshotId
    */
  protected[snapshot] def findBySnapshotId(workspaceId: UUID, snapshotId: UUID, userOffset: Int, userLimit: Int, batchSize: Int = 200): SnapshotListResponse = {

    val snapshotIdCriteria = snapshotId.toString // just so we're not calling toString on every iteration through loops

    @tailrec
    def findInPage(offset: Int, alreadyFound: List[DataRepoSnapshotResource]): List[DataRepoSnapshotResource] = {
      // get this page of references from WSM
      val newPage = retrieveSnapshotReferences(workspaceId, offset, batchSize)
      // filter the page to just those with a matching snapshotId
      val found = newPage.gcpDataRepoSnapshots.filter{ res =>
        Try(res.getAttributes.getSnapshot.equals(snapshotIdCriteria)).toOption.getOrElse(false)
      }
      // append the ones we just found to those found on previous pages
      val accum = alreadyFound ++ found

      if (newPage.gcpDataRepoSnapshots.size < batchSize || accum.size >= userOffset+userLimit) {
        // the page we retrieved from WSM was the last page, OR we have already found enough results
        // to satisfy the user's requested pagination criteria; return everything we found so far
        accum.slice(userOffset, userOffset+userLimit)
      } else {
        // the page we retrieved from WSM was NOT the last page; continue looping through the next page
        findInPage(offset + batchSize, accum)
      }
    }

    val refs = findInPage(0, List.empty[DataRepoSnapshotResource])

    SnapshotListResponse(refs)
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
      workspaceManagerDAO.getDataRepoSnapshotReference(workspaceContext.workspaceIdAsUUID, snapshotUuid, userInfo.accessToken)
      workspaceManagerDAO.deleteDataRepoSnapshotReference(workspaceContext.workspaceIdAsUUID, snapshotUuid, userInfo.accessToken)
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

}
