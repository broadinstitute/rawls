package org.broadinstitute.dsde.rawls.workspace

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.workspace.model.CreateCloudContextResult
import bio.terra.workspace.model.JobReport.StatusEnum
import com.typesafe.scalalogging.LazyLogging
import io.opencensus.scala.Tracing.traceWithParent
import io.opencensus.trace.Span
import org.broadinstitute.dsde.rawls.config.MultiCloudWorkspaceConfig
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{AttributeTempTableType, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{AttributeName, ErrorReport, GoogleProjectId, MultiCloudWorkspaceRequest, UserInfo, Workspace, WorkspaceAttributeSpecs, WorkspaceShardStates, WorkspaceType, WorkspaceVersions}
import org.broadinstitute.dsde.rawls.util.OpenCensusDBIOUtils.traceDBIOWithParent
import org.broadinstitute.dsde.rawls.util.Retry
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.joda.time.DateTime
import slick.dbio.DBIO
import slick.jdbc.TransactionIsolation

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

object MultiCloudWorkspaceService {
  def constructor(dataSource: SlickDataSource,
                  workspaceManagerDAO: WorkspaceManagerDAO,
                  multiCloudWorkspaceConfig: MultiCloudWorkspaceConfig)
                 (userInfo: UserInfo)
                 (implicit ec: ExecutionContext, system: ActorSystem): MultiCloudWorkspaceService = {
    new MultiCloudWorkspaceService(
      userInfo,
      workspaceManagerDAO,
      multiCloudWorkspaceConfig,
      dataSource
    )
  }
}


/**
  * This service knows how to provision a new "multi-cloud" workspace, a workspace managed by terra-workspace-manager.
  */
class MultiCloudWorkspaceService(userInfo: UserInfo,
                                 workspaceManagerDAO: WorkspaceManagerDAO,
                                 multiCloudWorkspaceConfig: MultiCloudWorkspaceConfig,
                                 dataSource: SlickDataSource)
                                (implicit ec: ExecutionContext, val system: ActorSystem) extends LazyLogging with Retry {

  def createMultiCloudWorkspace(workspaceRequest: MultiCloudWorkspaceRequest, parentSpan: Span = null) = {
    if (!multiCloudWorkspaceConfig.multiCloudWorkspacesEnabled) {
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotImplemented, "MC workspaces are not enabled"))
    }

    for {
      workspace <- traceWithParent("withNewMcWorkspaceContext", parentSpan)(
        opSpan => dataSource.inTransactionWithAttrTempTable(Set(AttributeTempTableType.Workspace))({ dataAccess =>
          createWorkspace(workspaceRequest, dataAccess, opSpan) { createdWorkspace =>
            DBIO.successful(createdWorkspace)
          }
        }, TransactionIsolation.ReadCommitted)) // read committed to avoid deadlocks on workspace attr scratch table
    } yield workspace

  }

  private def createWorkspace[T](workspaceRequest: MultiCloudWorkspaceRequest, dataAccess: DataAccess, parentSpan: Span)
                                (op: (Workspace) => ReadWriteAction[T]): ReadWriteAction[T] = {
    // TODO these will come from the spend profile service in the future
    val spendProfileId = multiCloudWorkspaceConfig.spendProfileId
    val azureTenantId = multiCloudWorkspaceConfig.azureTenantId
    val azureSubscriptionId = multiCloudWorkspaceConfig.azureSubscriptionId
    val azureResourceGroupId = multiCloudWorkspaceConfig.azureResourceGroupId

    traceDBIOWithParent("findByName", parentSpan)(_ => dataAccess.workspaceQuery.findByName(workspaceRequest.toWorkspaceName, Option(WorkspaceAttributeSpecs(all = false, List.empty[AttributeName])))) flatMap {
      case Some(_) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"Workspace ${workspaceRequest.namespace}/${workspaceRequest.name} already exists")))
      case None => {
        val workspaceId = UUID.randomUUID
        for {
          _ <- traceDBIOWithParent("createWorkspaceInWSM", parentSpan)(_ => DBIO.from(
            Future(workspaceManagerDAO.createWorkspaceWithSpendProfile(workspaceId, workspaceRequest.name, spendProfileId, userInfo.accessToken))
          ))
          _ = logger.info(s"Creating cloud context in WSM [workspaceId = ${workspaceId}]")
          cloudContextCreateResult <- traceDBIOWithParent("createCloudContext", parentSpan)(_ => DBIO.from(
            Future(workspaceManagerDAO.createAzureWorkspaceCloudContext(workspaceId, azureTenantId, azureResourceGroupId, azureSubscriptionId, userInfo.accessToken))
          ))
          jobControlId = cloudContextCreateResult.getJobReport.getId
          _ = logger.info(s"Polling on cloud context in WSM [jobControlId = ${jobControlId}]")
          pollingResult <- traceDBIOWithParent("pollCloudContext", parentSpan)(_ => DBIO.from(
            pollCloudContext(workspaceId, cloudContextCreateResult.getJobReport.getId, userInfo.accessToken)
          ))
          _ = logger.info(s"Creating workspace record [workspaceId = ${workspaceId}]")
          savedWorkspace <- traceDBIOWithParent("saveNewWorkspaceToRawlsDB", parentSpan)(_ => {
            createMultiCloudWorkspaceInDatabase(
              workspaceId.toString,
              workspaceRequest,
              dataAccess,
              parentSpan)
          })
          response <- traceDBIOWithParent("doOp", parentSpan)(_ => op(savedWorkspace))
        } yield {
          response
        }
      }
    }
  }

  private def getCloudContextCreationStatus(workspaceId: UUID,
                                            jobControlId: String,
                                            accessToken: OAuth2BearerToken): Future[CreateCloudContextResult] = {
    val result = workspaceManagerDAO.getWorkspaceCreateCloudContextResult(
      workspaceId, jobControlId, accessToken
    )
    result.getJobReport.getStatus match {
      case StatusEnum.SUCCEEDED => Future.successful(result)
      case _ => Future.failed(new WorkspaceManagerPollingOperationException(result.getJobReport.getStatus))
    }
  }

  private def jobStatusPredicate(t: Throwable): Boolean = {
    t match {
      case t: WorkspaceManagerPollingOperationException => (t.status == StatusEnum.RUNNING)
      case _ => false
    }
  }

  private def pollCloudContext(workspaceId: UUID, jobControlId: String, accessToken: OAuth2BearerToken): Future[Unit] = {
    for {
      result <- retryUntilSuccessOrTimeout(pred = jobStatusPredicate)(2 seconds, multiCloudWorkspaceConfig.cloudContextPollTimeout) {
        () => getCloudContextCreationStatus(workspaceId, jobControlId, accessToken)
      }
    } yield {
      result match {
        case Left(_) => throw new CloudContextCreationFailureException(
          s"Cloud context creation failed [workspaceId=${workspaceId}, jobControlId=${jobControlId}]",
          workspaceId,
          jobControlId
        )
        case Right(_) => ()
      }
    }
  }

  private def createMultiCloudWorkspaceInDatabase(workspaceId: String,
                                                  workspaceRequest: MultiCloudWorkspaceRequest,
                                                  dataAccess: DataAccess,
                                                  parentSpan: Span = null): ReadWriteAction[Workspace] = {
    val currentDate = DateTime.now
    val workspace = Workspace(
      namespace = workspaceRequest.namespace,
      name = workspaceRequest.name,
      workspaceId = workspaceId,
      bucketName = "",
      workflowCollectionName = Some(workspaceId),
      createdDate = currentDate,
      lastModified = currentDate,
      createdBy = userInfo.userEmail.value,
      attributes = workspaceRequest.attributes,
      isLocked = false,
      workspaceVersion = WorkspaceVersions.V2,
      googleProjectId = GoogleProjectId(""),
      googleProjectNumber = None,
      currentBillingAccountOnGoogleProject = None,
      billingAccountErrorMessage = None,
      completedCloneWorkspaceFileTransfer = None,
      shardState = WorkspaceShardStates.Sharded,
      workspaceType = WorkspaceType.McWorkspace
    )
    traceDBIOWithParent("saveMultiCloudWorkspace", parentSpan)(_ => dataAccess.workspaceQuery.createOrUpdate(workspace))
      .map(_ => workspace)
  }
}

class CloudContextCreationFailureException(message: String,
                                           val workspaceId: UUID,
                                           val jobControlId: String) extends RawlsException(message)
class WorkspaceManagerPollingOperationException(val status: StatusEnum) extends Exception
