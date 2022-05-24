package org.broadinstitute.dsde.rawls.workspace

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.workspace.model.JobReport.StatusEnum
import bio.terra.workspace.model.{CreateCloudContextResult, CreateControlledAzureRelayNamespaceResult}
import com.typesafe.scalalogging.LazyLogging
import io.opencensus.scala.Tracing.traceWithParent
import io.opencensus.trace.Span
import org.broadinstitute.dsde.rawls.config.MultiCloudWorkspaceConfig
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{ErrorReport, MultiCloudWorkspaceRequest, SamBillingProjectActions, SamResourceTypeNames, UserInfo, Workspace, WorkspaceCloudPlatform, WorkspaceRequest}
import org.broadinstitute.dsde.rawls.util.OpenCensusDBIOUtils.traceDBIOWithParent
import org.broadinstitute.dsde.rawls.util.Retry
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.joda.time.DateTime
import slick.jdbc.TransactionIsolation

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

object MultiCloudWorkspaceService {
  def constructor(dataSource: SlickDataSource,
                  workspaceManagerDAO: WorkspaceManagerDAO,
                  samDAO: SamDAO,
                  multiCloudWorkspaceConfig: MultiCloudWorkspaceConfig)
                 (userInfo: UserInfo)
                 (implicit ec: ExecutionContext, system: ActorSystem): MultiCloudWorkspaceService = {
    new MultiCloudWorkspaceService(
      userInfo,
      workspaceManagerDAO,
      samDAO,
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
                                 samDAO: SamDAO,
                                 multiCloudWorkspaceConfig: MultiCloudWorkspaceConfig,
                                 dataSource: SlickDataSource)
                                (implicit ec: ExecutionContext, val system: ActorSystem) extends LazyLogging with Retry {

  /**
   * Creates either a multi-cloud workspace (solely azure for now), or a rawls workspace.
   *
   * The determination is made by the choice of billing project in the request: if it's an Azure billing
   * project, this class handles the workspace creation. If not, delegates to the legacy WorksapceService codepath.
   * @param workspaceRequest Incoming workspace creation request
   * @param workspaceService Workspace service that will handle legacy creation requests
   * @param parentSpan OpenCensus span
   * @return Future containing the created Workspace's information
   */
  def createMultiCloudOrRawlsWorkspace(workspaceRequest: WorkspaceRequest,
                                       workspaceService: WorkspaceService,
                                       parentSpan: Span = null): Future[Workspace]= {
    val azureConfig = multiCloudWorkspaceConfig.azureConfig match {
      // no azure config, just create the workspace using the legacy codepath
      case None => return workspaceService.createWorkspace(workspaceRequest, parentSpan)
      case Some(value) => value
    }

    // for now, the only supported azure billing project is the hardcoded one from the config
    if (workspaceRequest.namespace == azureConfig.billingProjectName) {
      createMultiCloudWorkspace(
        MultiCloudWorkspaceRequest(
          workspaceRequest.namespace,
          workspaceRequest.name,
          workspaceRequest.attributes,
          WorkspaceCloudPlatform.Azure,
          azureConfig.defaultRegion
        )
      )
    } else {
      workspaceService.createWorkspace(workspaceRequest, parentSpan)
    }
  }

  /**
   * Creates a "multi-cloud" workspace, one that is managed by Workspace Manager.
   * @param workspaceRequest Workspace creation object
   * @param parentSpan OpenCensus span
   * @return Future containing the created Workspace's information
   */
  def createMultiCloudWorkspace(workspaceRequest: MultiCloudWorkspaceRequest, parentSpan: Span = null): Future[Workspace] = {
    if (!multiCloudWorkspaceConfig.multiCloudWorkspacesEnabled) {
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotImplemented, "MC workspaces are not enabled"))
    }

    traceWithParent("createMultiCloudWorkspace", parentSpan)(s1 =>
        createWorkspace(workspaceRequest, s1)
    )
  }

  private def createWorkspace(workspaceRequest: MultiCloudWorkspaceRequest, parentSpan: Span): Future[Workspace] = {
    val azureConfig = multiCloudWorkspaceConfig.azureConfig.getOrElse(throw new RawlsException("Azure config not present"))
    val wsmConfig = multiCloudWorkspaceConfig.workspaceManager.getOrElse(throw new RawlsException("WSM app config not present"))

    // TODO these will come from the spend profile service in the future
    val spendProfileId = azureConfig.spendProfileId
    val azureTenantId = azureConfig.azureTenantId
    val azureSubscriptionId = azureConfig.azureSubscriptionId
    val azureResourceGroupId = azureConfig.azureResourceGroupId

    val workspaceId = UUID.randomUUID
    for {
      _ <- samDAO.userHasAction(SamResourceTypeNames.billingProject, workspaceRequest.namespace, SamBillingProjectActions.createWorkspace, userInfo).flatMap {
        case true => Future.successful()
        case false => Future.failed(
          new RawlsExceptionWithErrorReport(
            errorReport = ErrorReport(
              StatusCodes.Forbidden,
              s"You are not authorized to create a workspace in billing project ${workspaceRequest.namespace}")
          ))
      }
      _ <- dataSource.inTransaction { dataAccess => dataAccess.workspaceQuery.findByName(workspaceRequest.toWorkspaceName) }.flatMap {
        case Some(_) => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"Workspace ${workspaceRequest.namespace}/${workspaceRequest.name} already exists")))
        case None => Future.successful()
      }
      _ <- traceWithParent("createMultiCloudWorkspaceInWSM", parentSpan)(_ =>
        Future(workspaceManagerDAO.createWorkspaceWithSpendProfile(workspaceId, workspaceRequest.name, spendProfileId, userInfo.accessToken))
      )
      _ = logger.info(s"Creating cloud context in WSM [workspaceId = ${workspaceId}]")
      cloudContextCreateResult <- traceWithParent("createAzureCloudContextInWSM", parentSpan)(_ =>
        Future(workspaceManagerDAO.createAzureWorkspaceCloudContext(workspaceId, azureTenantId, azureResourceGroupId, azureSubscriptionId, userInfo.accessToken))
      )
      jobControlId = cloudContextCreateResult.getJobReport.getId
      _ = logger.info(s"Polling on cloud context in WSM [workspaceId = ${workspaceId}, jobControlId = ${jobControlId}]")
      _ <- traceWithParent("pollGetCloudContextCreationStatusInWSM", parentSpan)(_ =>
        pollWMCreation(workspaceId, cloudContextCreateResult.getJobReport.getId, userInfo.accessToken, 2 seconds,
          wsmConfig.pollTimeout, "Cloud context", getCloudContextCreationStatus
        )
      )
      _ = logger.info(s"Creating workspace record [workspaceId = ${workspaceId}]")
      savedWorkspace: Workspace <- traceWithParent("saveMultiCloudWorkspaceToDB", parentSpan)(_ => dataSource.inTransaction({ dataAccess =>
        createMultiCloudWorkspaceInDatabase(
          workspaceId.toString,
          workspaceRequest,
          dataAccess,
          parentSpan)
      }, TransactionIsolation.ReadCommitted)
      )
      _ = logger.info(s"Enabling leonardo app in WSM [workspaceId = ${workspaceId}]")
      _ <- traceWithParent("enableLeoInWSM", parentSpan)(_ =>
        Future(workspaceManagerDAO.enableApplication(workspaceId, wsmConfig.leonardoWsmApplicationId, userInfo.accessToken))
      )
      _ = logger.info(s"Creating Azure relay in WSM [workspaceId = ${workspaceId}]")
      azureRelayCreateResult <- traceWithParent("createAzureRelayInWSM", parentSpan)(_ =>
        Future(workspaceManagerDAO.createAzureRelay(workspaceId, workspaceRequest.region, userInfo.accessToken))
      )
      // Create storage account before polling on relay because it takes ~45 seconds to create a relay
      _ = logger.info(s"Creating Azure storage account in WSM [workspaceId = ${workspaceId}]")
      _ <- traceWithParent("createStorageAccount", parentSpan)(_ =>
        Future(workspaceManagerDAO.createAzureStorageAccount(workspaceId, workspaceRequest.region, userInfo.accessToken))
      )
      relayJobControlId = azureRelayCreateResult.getJobReport.getId
      _ = logger.info(s"Polling on Azure relay in WSM [workspaceId = ${workspaceId}, jobControlId = ${relayJobControlId}]")
      _ <- traceWithParent("pollGetAzureRelayCreationStatusInWSM", parentSpan)(_ =>
        pollWMCreation(workspaceId, relayJobControlId, userInfo.accessToken, 5 seconds,
          wsmConfig.pollTimeout, "Azure relay", getAzureRelayCreationStatus)
      )
    } yield {
      savedWorkspace
    }
  }

  private def getCloudContextCreationStatus(workspaceId: UUID,
                                            jobControlId: String,
                                            accessToken: OAuth2BearerToken): Future[CreateCloudContextResult] = {
    val result = workspaceManagerDAO.getWorkspaceCreateCloudContextResult(workspaceId, jobControlId, accessToken)
    result.getJobReport.getStatus match {
      case StatusEnum.SUCCEEDED => Future.successful(result)
      case _ => Future.failed(new WorkspaceManagerPollingOperationException(
        s"Polling cloud context [jobControlId = ${jobControlId}] for status to be ${StatusEnum.SUCCEEDED}. Current status: ${result.getJobReport.getStatus}.",
        result.getJobReport.getStatus
      ))
    }
  }

  private def getAzureRelayCreationStatus(workspaceId: UUID,
                                            jobControlId: String,
                                            accessToken: OAuth2BearerToken): Future[CreateControlledAzureRelayNamespaceResult] = {
    val result = workspaceManagerDAO.getCreateAzureRelayResult(workspaceId, jobControlId, accessToken)
    result.getJobReport.getStatus match {
      case StatusEnum.SUCCEEDED => Future.successful(result)
      case _ => Future.failed(new WorkspaceManagerPollingOperationException(
        s"Polling Azure relay [jobControlId = ${jobControlId}] for status to be ${StatusEnum.SUCCEEDED}. Current status: ${result.getJobReport.getStatus}.",
        result.getJobReport.getStatus
      ))
    }
  }

  private def jobStatusPredicate(t: Throwable): Boolean = {
    t match {
      case t: WorkspaceManagerPollingOperationException => (t.status == StatusEnum.RUNNING)
      case _ => false
    }
  }

  private def pollWMCreation(workspaceId: UUID, jobControlId: String, accessToken: OAuth2BearerToken,
                             interval: FiniteDuration, pollTimeout: FiniteDuration, resourceType: String,
                             getCreationStatus:(UUID, String, OAuth2BearerToken) => Future[Object]): Future[Unit] = {
    for {
      result <- retryUntilSuccessOrTimeout(pred = jobStatusPredicate)(interval, pollTimeout) {
        () => getCreationStatus(workspaceId, jobControlId, accessToken)
      }
    } yield {
      result match {
        case Left(_) => throw new WorkspaceManagerCreationFailureException(
          s"${resourceType} failed [workspaceId=${workspaceId}, jobControlId=${jobControlId}]",
          workspaceId, jobControlId
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
      createdDate = currentDate,
      lastModified = currentDate,
      createdBy = userInfo.userEmail.value,
      attributes = workspaceRequest.attributes
    )
    traceDBIOWithParent("saveMultiCloudWorkspace", parentSpan)(_ => dataAccess.workspaceQuery.createOrUpdate(workspace))
      .map(_ => workspace)
  }
}

class WorkspaceManagerCreationFailureException(message: String,
                                               val workspaceId: UUID,
                                               val jobControlId: String) extends RawlsException(message)

class WorkspaceManagerPollingOperationException(message: String, val status: StatusEnum) extends Exception(message)


