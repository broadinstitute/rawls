package org.broadinstitute.dsde.rawls.workspace

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import bio.terra.profile.model.{CloudPlatform, ProfileModel}
import bio.terra.workspace.model.JobReport.StatusEnum
import bio.terra.workspace.model.{CreateCloudContextResult, CreateControlledAzureRelayNamespaceResult}
import cats.Apply
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.billing.BillingProfileManagerDAO
import org.broadinstitute.dsde.rawls.config.MultiCloudWorkspaceConfig
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.WorkspaceType.{McWorkspace, RawlsWorkspace}
import org.broadinstitute.dsde.rawls.model.{
  AzureManagedAppCoordinates,
  ErrorReport,
  MultiCloudWorkspaceRequest,
  RawlsBillingProject,
  RawlsBillingProjectName,
  RawlsRequestContext,
  SamWorkspaceActions,
  Workspace,
  WorkspaceCloudPlatform,
  WorkspaceName,
  WorkspaceRequest
}
import org.broadinstitute.dsde.rawls.util.TracingUtils.{traceDBIOWithParent, traceWithParent}
import org.broadinstitute.dsde.rawls.util.{Retry, WorkspaceSupport}
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.joda.time.DateTime
import slick.jdbc.TransactionIsolation

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{blocking, ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.{Success, Try}

object MultiCloudWorkspaceService {
  def constructor(dataSource: SlickDataSource,
                  workspaceManagerDAO: WorkspaceManagerDAO,
                  billingProfileManagerDAO: BillingProfileManagerDAO,
                  samDAO: SamDAO,
                  multiCloudWorkspaceConfig: MultiCloudWorkspaceConfig,
                  workbenchMetricBaseName: String
  )(ctx: RawlsRequestContext)(implicit ec: ExecutionContext, system: ActorSystem): MultiCloudWorkspaceService =
    new MultiCloudWorkspaceService(
      ctx,
      workspaceManagerDAO,
      billingProfileManagerDAO,
      samDAO,
      multiCloudWorkspaceConfig,
      dataSource,
      workbenchMetricBaseName
    )
}

/**
  * This service knows how to provision a new "multi-cloud" workspace, a workspace managed by terra-workspace-manager.
  */
class MultiCloudWorkspaceService(override val ctx: RawlsRequestContext,
                                 workspaceManagerDAO: WorkspaceManagerDAO,
                                 billingProfileManagerDAO: BillingProfileManagerDAO,
                                 override val samDAO: SamDAO,
                                 multiCloudWorkspaceConfig: MultiCloudWorkspaceConfig,
                                 override val dataSource: SlickDataSource,
                                 override val workbenchMetricBaseName: String
)(implicit override val executionContext: ExecutionContext, val system: ActorSystem)
    extends LazyLogging
    with RawlsInstrumented
    with Retry
    with WorkspaceSupport {

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
                                       parentContext: RawlsRequestContext = ctx
  ): Future[Workspace] =
    for {
      billingProject <- traceWithParent("getBillingProjectContext", parentContext) { s =>
        getBillingProjectContext(RawlsBillingProjectName(workspaceRequest.namespace), s)
      }

      billingProfileOpt <- traceWithParent("getBillingProfile", parentContext) { s =>
        getBillingProfile(billingProject, s)
      }

      workspaceOpt <- Apply[Option]
        .product(multiCloudWorkspaceConfig.azureConfig, billingProfileOpt)
        .traverse { case (azureConfig, profileModel) =>
          // "MultiCloud" workspaces are limited to azure-hosted workspaces for now.
          // This will likely change when the functionality for GCP workspaces gets moved out of Rawls
          Option(profileModel.getCloudPlatform)
            .filter(_ == CloudPlatform.AZURE)
            .traverse { _ =>
              traceWithParent("createMultiCloudWorkspace", parentContext) { s =>
                createMultiCloudWorkspace(
                  MultiCloudWorkspaceRequest(
                    workspaceRequest.namespace,
                    workspaceRequest.name,
                    workspaceRequest.attributes,
                    WorkspaceCloudPlatform.Azure,
                    azureConfig.defaultRegion,
                    AzureManagedAppCoordinates(
                      profileModel.getTenantId,
                      profileModel.getSubscriptionId,
                      profileModel.getManagedResourceGroupId
                    ),
                    profileModel.getId.toString
                  ),
                  s
                )
              }
            }
        }

      // Default to the legacy implementation if no workspace was been created
      // This can happen if there's
      // - no azure config
      // - no billing profile or the billing profile's cloud platform is Azure
      workspace <- workspaceOpt.flatten
        .map(Future.successful)
        .getOrElse(
          traceWithParent("createWorkspace", parentContext) { s =>
            workspaceService.createWorkspace(workspaceRequest, s)
          }
        )
    } yield workspace

  /**
    * Returns the billing profile associated with the billing project, if the billing project
    * has one. Fails if the billing profile id is specified and is malformed or does not exist.
    */
  def getBillingProfile(billingProject: RawlsBillingProject,
                        parentContext: RawlsRequestContext = ctx
  ): Future[Option[ProfileModel]] =
    billingProject.billingProfileId.traverse { profileIdString =>
      for {
        // bad state - the billing profile id got corrupted somehow
        profileId <- Try(UUID.fromString(profileIdString))
          .map(Future.successful)
          .getOrElse(
            Future.failed(
              RawlsExceptionWithErrorReport(
                ErrorReport(
                  StatusCodes.InternalServerError,
                  s"Invalid billing profile id '$profileIdString' on billing project '${billingProject.projectName}'."
                )
              )
            )
          )

        // fail if the billing project lists a billing profile that doesn't exist
        profileModel <- traceWithParent("getBillingProfile", parentContext) { s =>
          Future(blocking {
            billingProfileManagerDAO
              .getBillingProfile(profileId, s)
              .getOrElse(
                throw RawlsExceptionWithErrorReport(
                  ErrorReport(
                    StatusCodes.InternalServerError,
                    s"Unable to find billing profile with billingProfileId: $profileId"
                  )
                )
              )
          })
        }
      } yield profileModel
    }

  def cloneMultiCloudWorkspace(wsService: WorkspaceService,
                               sourceWorkspaceName: WorkspaceName,
                               destWorkspaceRequest: WorkspaceRequest
  ): Future[Workspace] =
    for {
      sourceWs <- getWorkspaceContextAndPermissions(sourceWorkspaceName, SamWorkspaceActions.read)
      billingProject <- getBillingProjectContext(RawlsBillingProjectName(destWorkspaceRequest.namespace))
      _ <- requireCreateWorkspaceAction(billingProject.projectName)
      _ <- dataSource.inTransaction(_ => failIfWorkspaceExists(destWorkspaceRequest.toWorkspaceName))
      billingProfileOpt <- getBillingProfile(billingProject)
      clone <- (sourceWs.workspaceType, billingProfileOpt) match {

        case (McWorkspace, Some(profile)) if profile.getCloudPlatform == CloudPlatform.AZURE =>
          cloneAzureWorkspace(sourceWs, billingProject, profile, destWorkspaceRequest)

        case (RawlsWorkspace, profileOpt)
            if profileOpt.isEmpty ||
              profileOpt.map(_.getCloudPlatform).contains(CloudPlatform.GCP) =>
          wsService.cloneWorkspace(sourceWs, billingProject, destWorkspaceRequest)

        case (wsType, profileOpt) =>
          Future.failed(
            RawlsExceptionWithErrorReport(
              ErrorReport(
                StatusCodes.BadRequest,
                s"Cloud platform mismatch: Cannot clone $wsType workspace '$sourceWorkspaceName' " +
                  s"into billing project '${billingProject.projectName}' " +
                  s"(hosted on ${profileOpt.map(_.getCloudPlatform).getOrElse(CloudPlatform.GCP)})."
              )
            )
          )
      }
    } yield clone

  def cloneAzureWorkspace(sourceWorkspace: Workspace,
                          project: RawlsBillingProject,
                          profile: ProfileModel,
                          request: WorkspaceRequest
  ): Future[Workspace] = Future.failed(
    RawlsExceptionWithErrorReport(
      ErrorReport(StatusCodes.NotImplemented, "WOR-625")
    )
  )

  /**
   * Creates a "multi-cloud" workspace, one that is managed by Workspace Manager.
   * @param workspaceRequest Workspace creation object
   * @param parentSpan OpenCensus span
   * @return Future containing the created Workspace's information
   */
  def createMultiCloudWorkspace(workspaceRequest: MultiCloudWorkspaceRequest,
                                parentContext: RawlsRequestContext = ctx
  ): Future[Workspace] = {
    if (!multiCloudWorkspaceConfig.multiCloudWorkspacesEnabled) {
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotImplemented, "MC workspaces are not enabled"))
    }

    traceWithParent("createMultiCloudWorkspace", parentContext)(s1 =>
      createWorkspace(workspaceRequest, s1) andThen { case Success(_) =>
        createdMultiCloudWorkspaceCounter.inc()
      }
    )
  }

  private def createWorkspace(workspaceRequest: MultiCloudWorkspaceRequest,
                              parentContext: RawlsRequestContext
  ): Future[Workspace] = {
    val wsmConfig = multiCloudWorkspaceConfig.workspaceManager
      .getOrElse(throw new RawlsException("WSM app config not present"))

    val spendProfileId = workspaceRequest.billingProfileId
    val azureTenantId = workspaceRequest.managedAppCoordinates.tenantId.toString
    val azureSubscriptionId = workspaceRequest.managedAppCoordinates.subscriptionId.toString
    val azureResourceGroupId = workspaceRequest.managedAppCoordinates.managedResourceGroupId

    val workspaceId = UUID.randomUUID
    for {
      _ <- requireCreateWorkspaceAction(RawlsBillingProjectName(workspaceRequest.namespace))
      _ <- dataSource.inTransaction(_ => failIfWorkspaceExists(workspaceRequest.toWorkspaceName))
      _ <- traceWithParent("createMultiCloudWorkspaceInWSM", parentContext)(_ =>
        Future(
          workspaceManagerDAO.createWorkspaceWithSpendProfile(workspaceId, workspaceRequest.name, spendProfileId, ctx)
        )
      )
      _ = logger.info(s"Creating cloud context in WSM [workspaceId = ${workspaceId}]")
      cloudContextCreateResult <- traceWithParent("createAzureCloudContextInWSM", parentContext)(_ =>
        Future(
          workspaceManagerDAO.createAzureWorkspaceCloudContext(workspaceId,
                                                               azureTenantId,
                                                               azureResourceGroupId,
                                                               azureSubscriptionId,
                                                               ctx
          )
        )
      )
      jobControlId = cloudContextCreateResult.getJobReport.getId
      _ = logger.info(s"Polling on cloud context in WSM [workspaceId = ${workspaceId}, jobControlId = ${jobControlId}]")
      _ <- traceWithParent("pollGetCloudContextCreationStatusInWSM", parentContext)(_ =>
        pollWMCreation(workspaceId,
                       cloudContextCreateResult.getJobReport.getId,
                       ctx,
                       2 seconds,
                       wsmConfig.pollTimeout,
                       "Cloud context",
                       getCloudContextCreationStatus
        )
      )
      _ = logger.info(s"Creating workspace record [workspaceId = ${workspaceId}]")
      savedWorkspace: Workspace <- traceWithParent("saveMultiCloudWorkspaceToDB", parentContext)(_ =>
        dataSource.inTransaction(
          dataAccess =>
            createMultiCloudWorkspaceInDatabase(workspaceId.toString, workspaceRequest, dataAccess, parentContext),
          TransactionIsolation.ReadCommitted
        )
      )
      _ = logger.info(s"Enabling leonardo app in WSM [workspaceId = ${workspaceId}]")
      _ <- traceWithParent("enableLeoInWSM", parentContext)(_ =>
        Future(workspaceManagerDAO.enableApplication(workspaceId, wsmConfig.leonardoWsmApplicationId, ctx))
      )
      _ = logger.info(s"Creating Azure relay in WSM [workspaceId = ${workspaceId}]")
      azureRelayCreateResult <- traceWithParent("createAzureRelayInWSM", parentContext)(_ =>
        Future(workspaceManagerDAO.createAzureRelay(workspaceId, workspaceRequest.region, ctx))
      )
      // Create storage container before polling on relay because it takes ~45 seconds to create a relay
      containerResult <- traceWithParent("createStorageContainer", parentContext)(_ =>
        Future(workspaceManagerDAO.createAzureStorageContainer(workspaceId, None, ctx))
      )
      _ = logger.info(
        s"Created Azure storage container in WSM [workspaceId = ${workspaceId}, containerId = ${containerResult.getResourceId}]"
      )
      relayJobControlId = azureRelayCreateResult.getJobReport.getId
      _ = logger.info(
        s"Polling on Azure relay in WSM [workspaceId = ${workspaceId}, jobControlId = ${relayJobControlId}]"
      )
      _ <- traceWithParent("pollGetAzureRelayCreationStatusInWSM", parentContext)(_ =>
        pollWMCreation(workspaceId,
                       relayJobControlId,
                       ctx,
                       5 seconds,
                       wsmConfig.pollTimeout,
                       "Azure relay",
                       getAzureRelayCreationStatus
        )
      )
    } yield savedWorkspace
  }

  private def getCloudContextCreationStatus(workspaceId: UUID,
                                            jobControlId: String,
                                            localCtx: RawlsRequestContext
  ): Future[CreateCloudContextResult] = {
    val result = workspaceManagerDAO.getWorkspaceCreateCloudContextResult(workspaceId, jobControlId, localCtx)
    result.getJobReport.getStatus match {
      case StatusEnum.SUCCEEDED => Future.successful(result)
      case _ =>
        Future.failed(
          new WorkspaceManagerPollingOperationException(
            s"Polling cloud context [jobControlId = ${jobControlId}] for status to be ${StatusEnum.SUCCEEDED}. Current status: ${result.getJobReport.getStatus}.",
            result.getJobReport.getStatus
          )
        )
    }
  }

  private def getAzureRelayCreationStatus(workspaceId: UUID,
                                          jobControlId: String,
                                          localCtx: RawlsRequestContext
  ): Future[CreateControlledAzureRelayNamespaceResult] = {
    val result = workspaceManagerDAO.getCreateAzureRelayResult(workspaceId, jobControlId, localCtx)
    result.getJobReport.getStatus match {
      case StatusEnum.SUCCEEDED => Future.successful(result)
      case _ =>
        Future.failed(
          new WorkspaceManagerPollingOperationException(
            s"Polling Azure relay [jobControlId = ${jobControlId}] for status to be ${StatusEnum.SUCCEEDED}. Current status: ${result.getJobReport.getStatus}.",
            result.getJobReport.getStatus
          )
        )
    }
  }

  private def jobStatusPredicate(t: Throwable): Boolean =
    t match {
      case t: WorkspaceManagerPollingOperationException => t.status == StatusEnum.RUNNING
      case _                                            => false
    }

  private def pollWMCreation(workspaceId: UUID,
                             jobControlId: String,
                             localCtx: RawlsRequestContext,
                             interval: FiniteDuration,
                             pollTimeout: FiniteDuration,
                             resourceType: String,
                             getCreationStatus: (UUID, String, RawlsRequestContext) => Future[Object]
  ): Future[Unit] =
    for {
      result <- retryUntilSuccessOrTimeout(pred = jobStatusPredicate)(interval, pollTimeout) { () =>
        getCreationStatus(workspaceId, jobControlId, localCtx)
      }
    } yield result match {
      case Left(_) =>
        throw new WorkspaceManagerCreationFailureException(
          s"${resourceType} failed [workspaceId=${workspaceId}, jobControlId=${jobControlId}]",
          workspaceId,
          jobControlId
        )
      case Right(_) => ()
    }

  private def createMultiCloudWorkspaceInDatabase(workspaceId: String,
                                                  workspaceRequest: MultiCloudWorkspaceRequest,
                                                  dataAccess: DataAccess,
                                                  parentContext: RawlsRequestContext
  ): ReadWriteAction[Workspace] = {
    val currentDate = DateTime.now
    val workspace = Workspace.buildMcWorkspace(
      namespace = workspaceRequest.namespace,
      name = workspaceRequest.name,
      workspaceId = workspaceId,
      createdDate = currentDate,
      lastModified = currentDate,
      createdBy = ctx.userInfo.userEmail.value,
      attributes = workspaceRequest.attributes
    )
    traceDBIOWithParent("saveMultiCloudWorkspace", parentContext)(_ =>
      dataAccess.workspaceQuery.createOrUpdate(workspace)
    )
      .map(_ => workspace)
  }
}

class WorkspaceManagerCreationFailureException(message: String, val workspaceId: UUID, val jobControlId: String)
    extends RawlsException(message)

class WorkspaceManagerPollingOperationException(message: String, val status: StatusEnum) extends Exception(message)
