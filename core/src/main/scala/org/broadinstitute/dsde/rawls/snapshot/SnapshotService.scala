package org.broadinstitute.dsde.rawls.snapshot

import akka.http.scaladsl.model.StatusCodes
import bio.terra.workspace.model._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.DataRepoDAO
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{
  DataReferenceName,
  ErrorReport,
  NamedDataRepoSnapshot,
  RawlsRequestContext,
  SamWorkspaceActions,
  SnapshotListResponse,
  Workspace,
  WorkspaceAttributeSpecs,
  WorkspaceName
}
import org.broadinstitute.dsde.rawls.util.{FutureSupport, WorkspaceSupport}

import java.util.UUID
import scala.annotation.tailrec
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

object SnapshotService {
  def constructor(dataSource: SlickDataSource,
                  samDAO: SamDAO,
                  workspaceManagerDAO: WorkspaceManagerDAO,
                  terraDataRepoUrl: String,
                  dataRepoDAO: DataRepoDAO
  )(ctx: RawlsRequestContext)(implicit executionContext: ExecutionContext): SnapshotService =
    new SnapshotService(ctx, dataSource, samDAO, workspaceManagerDAO, terraDataRepoUrl, dataRepoDAO)
}

class SnapshotService(protected val ctx: RawlsRequestContext,
                      val dataSource: SlickDataSource,
                      val samDAO: SamDAO,
                      workspaceManagerDAO: WorkspaceManagerDAO,
                      terraDataRepoInstanceName: String,
                      dataRepoDAO: DataRepoDAO
)(implicit protected val executionContext: ExecutionContext)
    extends FutureSupport
    with WorkspaceSupport
    with LazyLogging {

  def createSnapshot(workspaceName: WorkspaceName, snapshot: NamedDataRepoSnapshot): Future[DataRepoSnapshotResource] =
    getV2WorkspaceContextAndPermissions(workspaceName,
                                        SamWorkspaceActions.write,
                                        Some(WorkspaceAttributeSpecs(all = false))
    ).flatMap { workspaceContext =>
      val wsid = workspaceContext.workspaceIdAsUUID // to avoid UUID parsing multiple times
      // create the stub workspace in WSM if it does not already exist
      if (!workspaceStubExists(wsid, ctx)) {
        workspaceManagerDAO.createWorkspace(wsid, ctx)
      }
      validateProtectedStatus(workspaceContext, snapshot)
      // create the requested snapshot reference
      val snapshotRef = workspaceManagerDAO.createDataRepoSnapshotReference(wsid,
                                                                            snapshot.snapshotId,
                                                                            snapshot.name,
                                                                            snapshot.description,
                                                                            terraDataRepoInstanceName,
                                                                            CloningInstructionsEnum.NOTHING,
                                                                            ctx
      )
      Future.successful(snapshotRef)
    }

  def validateProtectedStatus(workspaceContext: Workspace, snapshot: NamedDataRepoSnapshot): Unit =
    // logically it might make more sense to check if the snapshot is protected before the workspace
    // but that is a more expensive check
    // check if workspace is protected
    if (!workspaceContext.bucketName.startsWith("fc-secure")) {
      // if not, check if snapshot is protected
      val sources = dataRepoDAO.getSnapshot(snapshot.snapshotId, ctx.userInfo.accessToken).getSource
      if(sources.exists(_.getDataset.isSecureMonitoringEnabled)) {
        throw new RawlsException("Unable to add protected snapshot to unprotected workspace.")
      }
      }
    }

  def getSnapshot(workspaceName: WorkspaceName, referenceId: String): Future[DataRepoSnapshotResource] = {
    val referenceUuid = validateSnapshotId(referenceId)
    getV2WorkspaceContextAndPermissions(workspaceName,
                                        SamWorkspaceActions.read,
                                        Some(WorkspaceAttributeSpecs(all = false))
    ).flatMap { workspaceContext =>
      val ref = workspaceManagerDAO.getDataRepoSnapshotReference(workspaceContext.workspaceIdAsUUID, referenceUuid, ctx)
      Future.successful(ref)
    }
  }

  def getSnapshotByName(workspaceName: WorkspaceName, referenceName: String): Future[DataRepoSnapshotResource] =
    getV2WorkspaceContextAndPermissions(workspaceName,
                                        SamWorkspaceActions.read,
                                        Some(WorkspaceAttributeSpecs(all = false))
    ).flatMap { workspaceContext =>
      val ref = workspaceManagerDAO.getDataRepoSnapshotReferenceByName(workspaceContext.workspaceIdAsUUID,
                                                                       DataReferenceName(referenceName),
                                                                       ctx
      )
      Future.successful(ref)
    }

  // AS-787 - rework the data so that it's in the same place in the JSON with a list and get snapshot responses
  def massageSnapshots(references: ResourceList): SnapshotListResponse = {
    val snapshots = references.getResources.asScala.map { r =>
      val massaged = new DataRepoSnapshotResource
      massaged.setAttributes(r.getResourceAttributes.getGcpDataRepoSnapshot)
      massaged.setMetadata(r.getMetadata)
      massaged
    }
    SnapshotListResponse(snapshots.toList)
  }

  /*
    internal method to query WSM for a list of snapshot references; used by enumerateSnapshots and findBySnapshotId
   */
  protected[snapshot] def retrieveSnapshotReferences(workspaceId: UUID, offset: Int, limit: Int): SnapshotListResponse =
    Try(workspaceManagerDAO.enumerateDataRepoSnapshotReferences(workspaceId, offset, limit, ctx)) match {
      case Success(references) => massageSnapshots(references)
      // if we fail with a 404, it means we have no stub in WSM yet. This is benign and functionally equivalent
      // to having no references, so return the empty list.
      case Failure(ex: bio.terra.workspace.client.ApiException) if ex.getCode == 404 =>
        new SnapshotListResponse(Seq.empty[DataRepoSnapshotResource])
      // but if we hit a different error, it's a valid error; rethrow it
      case Failure(ex: bio.terra.workspace.client.ApiException) =>
        throw new RawlsExceptionWithErrorReport(ErrorReport(ex.getCode, ex))
      case Failure(other) =>
        logger.warn(s"Unexpected error when enumerating snapshots: ${other.getMessage}")
        throw new RawlsExceptionWithErrorReport(ErrorReport(other))
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
  def enumerateSnapshots(workspaceName: WorkspaceName,
                         offset: Int,
                         limit: Int,
                         referencedSnapshotId: Option[UUID] = None
  ): Future[SnapshotListResponse] =
    getV2WorkspaceContextAndPermissions(workspaceName,
                                        SamWorkspaceActions.read,
                                        Some(WorkspaceAttributeSpecs(all = false))
    ).map { workspaceContext =>
      referencedSnapshotId match {
        case None     => retrieveSnapshotReferences(workspaceContext.workspaceIdAsUUID, offset, limit)
        case Some(id) => findBySnapshotId(workspaceContext.workspaceIdAsUUID, id, offset, limit)
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
  protected[snapshot] def findBySnapshotId(workspaceId: UUID,
                                           snapshotId: UUID,
                                           userOffset: Int,
                                           userLimit: Int,
                                           batchSize: Int = 200
  ): SnapshotListResponse = {

    val snapshotIdCriteria = snapshotId.toString // just so we're not calling toString on every iteration through loops

    @tailrec
    def findInPage(offset: Int, alreadyFound: List[DataRepoSnapshotResource]): List[DataRepoSnapshotResource] = {
      // get this page of references from WSM
      val newPage = retrieveSnapshotReferences(workspaceId, offset, batchSize)
      // filter the page to just those with a matching snapshotId
      val found = newPage.gcpDataRepoSnapshots.filter { res =>
        Try(res.getAttributes.getSnapshot.equals(snapshotIdCriteria)).toOption.getOrElse(false)
      }
      // append the ones we just found to those found on previous pages
      val accum = alreadyFound ++ found

      if (newPage.gcpDataRepoSnapshots.size < batchSize || accum.size >= userOffset + userLimit) {
        // the page we retrieved from WSM was the last page, OR we have already found enough results
        // to satisfy the user's requested pagination criteria; return everything we found so far
        accum.slice(userOffset, userOffset + userLimit)
      } else {
        // the page we retrieved from WSM was NOT the last page; continue looping through the next page
        findInPage(offset + batchSize, accum)
      }
    }

    val refs = findInPage(0, List.empty[DataRepoSnapshotResource])

    SnapshotListResponse(refs)
  }

  def updateSnapshot(workspaceName: WorkspaceName,
                     snapshotId: String,
                     updateInfo: UpdateDataRepoSnapshotReferenceRequestBody
  ): Future[Unit] = {
    val snapshotUuid = validateSnapshotId(snapshotId)
    getV2WorkspaceContextAndPermissions(workspaceName,
                                        SamWorkspaceActions.write,
                                        Some(WorkspaceAttributeSpecs(all = false))
    ).map { workspaceContext =>
      // check that snapshot exists before updating it. If the snapshot does not exist, the GET attempt will throw a 404
      workspaceManagerDAO.getDataRepoSnapshotReference(workspaceContext.workspaceIdAsUUID, snapshotUuid, ctx)
      // build the update request body, ignoring any changes to instanceName and snapshot, and requiring either name or description
      if (Option(updateInfo.getName).isEmpty && Option(updateInfo.getDescription).isEmpty) {
        throw new RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.BadRequest, "Either name or description is required.")
        )
      }
      val updateBody = new UpdateDataRepoSnapshotReferenceRequestBody()
      updateBody.setName(updateInfo.getName)
      updateBody.setDescription(updateInfo.getDescription)
      // perform the update
      workspaceManagerDAO.updateDataRepoSnapshotReference(workspaceContext.workspaceIdAsUUID,
                                                          snapshotUuid,
                                                          updateBody,
                                                          ctx
      )
    }
  }

  def deleteSnapshot(workspaceName: WorkspaceName, snapshotId: String): Future[Unit] = {
    val snapshotUuid = validateSnapshotId(snapshotId)
    getV2WorkspaceContextAndPermissions(workspaceName,
                                        SamWorkspaceActions.write,
                                        Some(WorkspaceAttributeSpecs(all = false))
    ).map { workspaceContext =>
      // check that snapshot exists before deleting it. If the snapshot does not exist, the GET attempt will throw a 404
      workspaceManagerDAO.getDataRepoSnapshotReference(workspaceContext.workspaceIdAsUUID, snapshotUuid, ctx)
      workspaceManagerDAO.deleteDataRepoSnapshotReference(workspaceContext.workspaceIdAsUUID, snapshotUuid, ctx)
    }
  }

  private def workspaceStubExists(workspaceId: UUID, ctx: RawlsRequestContext): Boolean =
    Try(workspaceManagerDAO.getWorkspace(workspaceId, ctx)).isSuccess

  private def validateSnapshotId(snapshotId: String): UUID =
    Try(UUID.fromString(snapshotId)) match {
      case Success(snapshotUuid) => snapshotUuid
      case Failure(_) =>
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "SnapshotId must be a valid UUID."))
    }

}
