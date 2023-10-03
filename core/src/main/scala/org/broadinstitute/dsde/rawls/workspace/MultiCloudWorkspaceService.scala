package org.broadinstitute.dsde.rawls.workspace

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import bio.terra.profile.model.{CloudPlatform, ProfileModel}
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model.JobReport.StatusEnum
import bio.terra.workspace.model._
import cats.Apply
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.billing.BillingProfileManagerDAO
import org.broadinstitute.dsde.rawls.config.MultiCloudWorkspaceConfig
import org.broadinstitute.dsde.rawls.dataaccess.slick.{
  DataAccess,
  ReadWriteAction,
  WorkspaceManagerResourceMonitorRecord
}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{
  LeonardoDAO,
  SamDAO,
  SlickDataSource,
  WorkspaceManagerResourceMonitorRecordDao
}
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.WorkspaceType.{McWorkspace, RawlsWorkspace}
import org.broadinstitute.dsde.rawls.model.{
  AttributeBoolean,
  AttributeName,
  AttributeString,
  ErrorReport,
  RawlsBillingProject,
  RawlsBillingProjectName,
  RawlsRequestContext,
  SamWorkspaceActions,
  Workspace,
  WorkspaceDeletionResult,
  WorkspaceName,
  WorkspaceRequest,
  WorkspaceState,
  WorkspaceType
}
import org.broadinstitute.dsde.rawls.util.TracingUtils.{traceDBIOWithParent, traceWithParent}
import org.broadinstitute.dsde.rawls.util.{Retry, WorkspaceSupport}
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.joda.time.{DateTime, DateTimeZone}

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{blocking, ExecutionContext, Future}
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}
import scala.jdk.CollectionConverters._

object MultiCloudWorkspaceService {
  def constructor(dataSource: SlickDataSource,
                  workspaceManagerDAO: WorkspaceManagerDAO,
                  billingProfileManagerDAO: BillingProfileManagerDAO,
                  samDAO: SamDAO,
                  multiCloudWorkspaceConfig: MultiCloudWorkspaceConfig,
                  leonardoDAO: LeonardoDAO,
                  workbenchMetricBaseName: String
  )(ctx: RawlsRequestContext)(implicit ec: ExecutionContext, system: ActorSystem): MultiCloudWorkspaceService =
    new MultiCloudWorkspaceService(
      ctx,
      workspaceManagerDAO,
      billingProfileManagerDAO,
      samDAO,
      multiCloudWorkspaceConfig,
      leonardoDAO,
      dataSource,
      workbenchMetricBaseName
    )

  def getStorageContainerName(workspaceId: UUID): String = s"sc-${workspaceId}"

}

/**
  * This service knows how to provision a new "multi-cloud" workspace, a workspace managed by terra-workspace-manager.
  */
class MultiCloudWorkspaceService(override val ctx: RawlsRequestContext,
                                 val workspaceManagerDAO: WorkspaceManagerDAO,
                                 billingProfileManagerDAO: BillingProfileManagerDAO,
                                 override val samDAO: SamDAO,
                                 val multiCloudWorkspaceConfig: MultiCloudWorkspaceConfig,
                                 val leonardoDAO: LeonardoDAO,
                                 override val dataSource: SlickDataSource,
                                 override val workbenchMetricBaseName: String
)(implicit override val executionContext: ExecutionContext, val system: ActorSystem)
    extends LazyLogging
    with RawlsInstrumented
    with Retry
    with WorkspaceSupport {

  /**
    * Deletes a workspace. For legacy "rawls" workspaces,
    * delegates to the WorkspaceService codepath. For MC workspaces (i.e., azure),
    * initiates a deletion with WorkspaceManager and then deletes the underlying rawls record.
    * @param workspaceName Tuple of workspace name and namespace
    * @param workspaceService WorkspaceService instance which will handle deletion of GCP workspaces
    * @return If GCP, name of the GCP bucket being deleted; otherwise None
    */
  def deleteMultiCloudOrRawlsWorkspace(workspaceName: WorkspaceName,
                                       workspaceService: WorkspaceService
  ): Future[Option[String]] =
    for {
      workspace <- getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.delete)
      _ = logger.info(
        s"Deleting workspace [workspaceId=${workspace.workspaceId}, name=${workspaceName.name}, billingProject=${workspace.namespace}, user=${ctx.userInfo.userSubjectId.value}]"
      )
      result: WorkspaceDeletionResult <- workspace.workspaceType match {
        case RawlsWorkspace => workspaceService.deleteWorkspace(workspaceName)
        case McWorkspace    => deleteMultiCloudWorkspace(workspace)
      }
    } yield result.gcpContext.map(_.bucketName)

  /**
    * Starts the deletion process for a workspace. For GCP workspaces, the deletion is complete synchronously.
    * MC workspaces are enqueued for deletion via an entry in the WorkspaceManagerMonitor record table, with
    * orchestration of the cleanup handed off to the WorkspaceDeletionRunner runner class.

    * @param workspaceName Tuple of namespace + name of the workspace for deletion
    * @param workspaceService Workspace service to which legacy GCP deletion calls will be delegated
    * @return Result of the deletion operation, including a job ID for async deletions
    */
  def deleteMultiCloudOrRawlsWorkspaceV2(workspaceName: WorkspaceName,
                                         workspaceService: WorkspaceService
  ): Future[WorkspaceDeletionResult] =
    for {
      workspace <- getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.delete)

      _ = logger.info(
        s"V2 Starting deletion of workspace [workspaceId=${workspace.workspaceId}, name=${workspaceName.name}, billingProject=${workspace.namespace}, user=${ctx.userInfo.userSubjectId.value}]"
      )

      _ = workspace.state match {
        case WorkspaceState.Ready | WorkspaceState.CreateFailed | WorkspaceState.DeleteFailed |
            WorkspaceState.UpdateFailed =>
          true
        case WorkspaceState.Deleting =>
          throw new RawlsExceptionWithErrorReport(
            ErrorReport(StatusCodes.Conflict, "Workspace is already being deleted.")
          )
        case _ =>
          logger.error(
            s"Unable to delete workspace [id = ${workspace.workspaceId}, current_state = ${workspace.state}]"
          )
          throw new RawlsExceptionWithErrorReport(
            ErrorReport(StatusCodes.BadRequest, s"Unable to delete workspace")
          )
      }

      result: WorkspaceDeletionResult <- workspace.workspaceType match {
        case WorkspaceType.McWorkspace    => startMultiCloudDelete(workspace, ctx)
        case WorkspaceType.RawlsWorkspace => workspaceService.deleteWorkspace(workspace.toWorkspaceName)
      }
    } yield result

  private def startMultiCloudDelete(workspace: Workspace,
                                    parentContext: RawlsRequestContext = ctx
  ): Future[WorkspaceDeletionResult] = {
    val jobId = UUID.randomUUID()
    for {
      _ <- dataSource.inTransaction { access =>
        access.workspaceQuery.updateState(workspace.workspaceIdAsUUID, WorkspaceState.Deleting)
      }
      _ <- WorkspaceManagerResourceMonitorRecordDao(dataSource).create(
        WorkspaceManagerResourceMonitorRecord.forWorkspaceDeletion(
          jobId,
          workspace.workspaceIdAsUUID,
          parentContext.userInfo.userEmail
        )
      )
    } yield WorkspaceDeletionResult(Some(jobId.toString), None)
  }

  private def deleteMultiCloudWorkspace(workspace: Workspace): Future[WorkspaceDeletionResult] =
    for {
      _ <- deleteWorkspaceInWSM(workspace.workspaceIdAsUUID).recover { case e: ApiException =>
        if (e.getCode == StatusCodes.NotFound.intValue) {
          // if the workspace is not present in WSM (likely already deleted), proceed with cleaning up rawls state
          logger.warn(
            s"Workspace not found in WSM for deletion, proceeding with deletion of Rawls state [workspaceId = ${workspace.workspaceId}]"
          )
        } else {
          throw new RawlsExceptionWithErrorReport(
            errorReport = ErrorReport(StatusCodes.InternalServerError,
                                      s"Unable to delete workspace [workspaceId=${workspace.workspaceId}]",
                                      ErrorReport(e)
            )
          )
        }
      }
      _ <- deleteWorkspaceRecord(workspace)
    } yield {
      deletedMultiCloudWorkspaceCounter.inc()
      logger.info(
        s"Deleted multi-cloud workspace " +
          s"[workspaceId=${workspace.workspaceIdAsUUID}, " +
          s"name=${workspace.name}, " +
          s"namespace=${workspace.namespace}]"
      )
      WorkspaceDeletionResult(None, None)
    }

  /**
    * Creates either a multi-cloud workspace (solely azure for now), or a rawls workspace.
    *
    * The determination is made by the choice of billing project in the request: if it's an Azure billing
    * project, this class handles the workspace creation. If not, delegates to the legacy WorkspaceService codepath.
    *
    * @param workspaceRequest Incoming workspace creation request
    * @param workspaceService Workspace service that will handle legacy creation requests
    * @param parentContext    Request context for tracing
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

      _ <- traceWithParent("requireCreateWorkspaceAccess", parentContext) { childContext =>
        requireCreateWorkspaceAction(billingProject.projectName, childContext)
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
                  workspaceRequest,
                  profileModel,
                  s
                )
              }
            }
        }

      // Default to the legacy implementation if no workspace was been created
      // This can happen if there's
      // - no azure config
      // - no billing profile or the billing profile's cloud platform is GCP
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
      sourceWs <- getV2WorkspaceContextAndPermissions(sourceWorkspaceName, SamWorkspaceActions.read)
      billingProject <- getBillingProjectContext(RawlsBillingProjectName(destWorkspaceRequest.namespace))
      _ <- requireCreateWorkspaceAction(billingProject.projectName)
      billingProfileOpt <- getBillingProfile(billingProject)
      clone <- (sourceWs.workspaceType, billingProfileOpt) match {

        case (McWorkspace, Some(profile)) if profile.getCloudPlatform == CloudPlatform.AZURE =>
          traceWithParent("cloneAzureWorkspace", ctx) { s =>
            cloneAzureWorkspace(sourceWs, profile, destWorkspaceRequest, s)
          }

        case (RawlsWorkspace, profileOpt)
            if profileOpt.isEmpty ||
              profileOpt.map(_.getCloudPlatform).contains(CloudPlatform.GCP) =>
          traceWithParent("cloneRawlsWorkspace", ctx) { s =>
            wsService.cloneWorkspace(sourceWs, billingProject, destWorkspaceRequest, s)
          }

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
                          profile: ProfileModel,
                          request: WorkspaceRequest,
                          parentContext: RawlsRequestContext
  ): Future[Workspace] = {

    assertBillingProfileCreationDate(profile)

    val wsmConfig = multiCloudWorkspaceConfig.workspaceManager
      .getOrElse(throw new RawlsException("WSM app config not present"))
    val workspaceId = UUID.randomUUID()

    for {
      // The call to WSM is asynchronous. Before we fire it off, allocate a new workspace record
      // to avoid naming conflicts - we'll erase it should the clone request to WSM fail.
      newWorkspace <- createNewWorkspaceRecord(workspaceId, request, parentContext)

      containerCloneResult <- (for {
        cloneResult <- traceWithParent("workspaceManagerDAO.cloneWorkspace", parentContext) { context =>
          Future(blocking {
            workspaceManagerDAO.cloneWorkspace(
              sourceWorkspaceId = sourceWorkspace.workspaceIdAsUUID,
              workspaceId = workspaceId,
              displayName = request.name,
              spendProfile = profile,
              billingProjectNamespace = request.namespace,
              context
            )
          })
        }
        jobControlId = cloneResult.getJobReport.getId
        _ = logger.info(
          s"Polling on workspace clone in WSM [workspaceId = ${workspaceId}, jobControlId = ${jobControlId}]"
        )
        _ <- traceWithParent("workspaceManagerDAO.getWorkspaceCloneStatus", parentContext) { context =>
          pollWMOperation(workspaceId,
                          jobControlId,
                          context,
                          2 seconds,
                          wsmConfig.pollTimeout,
                          "Clone workspace",
                          getWorkspaceCloneStatus
          )
        }

        _ = logger.info(
          s"Starting workspace storage container clone in WSM [workspaceId = ${workspaceId}]"
        )
        containerCloneResult <- traceWithParent("workspaceManagerDAO.cloneAzureStorageContainer", parentContext) {
          context =>
            cloneWorkspaceStorageContainer(sourceWorkspace.workspaceIdAsUUID,
                                           workspaceId,
                                           request.copyFilesWithPrefix,
                                           context
            )
        }

        // create a WDS application in Leo
        _ <- createWdsAppInWorkspace(workspaceId,
                                     parentContext,
                                     Some(sourceWorkspace.workspaceIdAsUUID),
                                     request.attributes
        )

      } yield containerCloneResult).recoverWith { t: Throwable =>
        logger.warn(
          "Clone workspace request to workspace manager failed for " +
            s"[ sourceWorkspaceName='${sourceWorkspace.toWorkspaceName}'" +
            s", newWorkspaceName='${newWorkspace.toWorkspaceName}'" +
            s"], Rawls record being deleted.",
          t
        )
        dataSource.inTransaction(_.workspaceQuery.delete(newWorkspace.toWorkspaceName)) >> Future.failed(t)
      }
      _ = clonedMultiCloudWorkspaceCounter.inc()
      _ = logger.info(
        "Created azure workspace " +
          s"[ workspaceId='${newWorkspace.workspaceId}'" +
          s", workspaceName='${newWorkspace.toWorkspaceName}'" +
          s", containerCloneJobReportId='${containerCloneResult.getJobReport.getId}'" +
          s"]"
      )

      // hand off monitoring the clone job to the resource monitor
      _ <- WorkspaceManagerResourceMonitorRecordDao(dataSource).create(
        WorkspaceManagerResourceMonitorRecord.forCloneWorkspaceContainer(
          UUID.fromString(containerCloneResult.getJobReport.getId),
          workspaceId,
          parentContext.userInfo.userEmail
        )
      )

    } yield newWorkspace
  }

  def cloneWorkspaceStorageContainer(sourceWorkspaceId: UUID,
                                     destinationWorkspaceId: UUID,
                                     prefixToClone: Option[String],
                                     ctx: RawlsRequestContext
  ): Future[CloneControlledAzureStorageContainerResult] = {

    val expectedContainerName = MultiCloudWorkspaceService.getStorageContainerName(sourceWorkspaceId)
    // Using limit of 200 to be safe, but we expect at most a handful of storage containers.
    val allContainers =
      workspaceManagerDAO.enumerateStorageContainers(sourceWorkspaceId, 0, 200, ctx).getResources.asScala
    val sharedAccessContainers = allContainers.filter(resource =>
      resource.getMetadata.getControlledResourceMetadata.getAccessScope == AccessScope.SHARED_ACCESS
    )
    if (sharedAccessContainers.size > 1) {
      logger.warn(
        s"Workspace being cloned has multiple shared access containers [ workspaceId='${sourceWorkspaceId}' ]"
      )
    }
    val container = sharedAccessContainers.find(resource => resource.getMetadata.getName == expectedContainerName)
    container match {
      case Some(_) =>
        Future(blocking {
          workspaceManagerDAO.cloneAzureStorageContainer(
            sourceWorkspaceId,
            destinationWorkspaceId,
            container.get.getMetadata.getResourceId,
            MultiCloudWorkspaceService.getStorageContainerName(destinationWorkspaceId),
            CloningInstructionsEnum.RESOURCE,
            prefixToClone,
            ctx
          )
        })
      case None =>
        Future.failed(
          RawlsExceptionWithErrorReport(
            ErrorReport(
              StatusCodes.InternalServerError,
              s"Workspace ${sourceWorkspaceId} does not have the expected storage container ${expectedContainerName}."
            )
          )
        )
    }
  }

  /**
    * Creates a "multi-cloud" workspace, one that is managed by Workspace Manager.
    *
    * @param workspaceRequest Workspace creation object
    * @param parentContext       Rawls request context
    * @return Future containing the created Workspace's information
    */
  def createMultiCloudWorkspace(workspaceRequest: WorkspaceRequest,
                                profile: ProfileModel,
                                parentContext: RawlsRequestContext = ctx
  ): Future[Workspace] = {
    if (!multiCloudWorkspaceConfig.multiCloudWorkspacesEnabled) {
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotImplemented, "MC workspaces are not enabled"))
    }

    assertBillingProfileCreationDate(profile)

    traceWithParent("createMultiCloudWorkspace", parentContext)(s1 =>
      createWorkspace(workspaceRequest, profile, s1) andThen { case Success(_) =>
        createdMultiCloudWorkspaceCounter.inc()
      }
    )
  }

  def assertBillingProfileCreationDate(profile: ProfileModel): Unit = {
    val previewDate = new DateTime(2023, 9, 12, 0, 0, DateTimeZone.UTC)
    val isTestProfile = profile.getCreatedDate() == null || profile.getCreatedDate() == ""
    if (!isTestProfile && DateTime.parse(profile.getCreatedDate()).isBefore(previewDate)) {
      throw new RawlsExceptionWithErrorReport(
        ErrorReport(
          StatusCodes.Forbidden,
          "Azure Terra billing projects created before 9/12/2023, and their associated workspaces, are unable to take advantage of new functionality. This billing project cannot be used for creating new workspaces. Please create a new billing project."
        )
      )
    }
  }

  /**
    * Starts a workspace deletion operation with workspace maanger and polls to completion
    * @param workspaceId
    * @return
    */
  def deleteWorkspaceInWSM(workspaceId: UUID): Future[Unit] = {
    val wsmConfig = multiCloudWorkspaceConfig.workspaceManager
      .getOrElse(throw new RawlsException("WSM app config not present"))
    getWorkspaceFromWsm(workspaceId, ctx).getOrElse(return Future.successful())
    for {
      // kick off the deletion job w/WSM
      deletionJobResult <- traceWithParent("deleteWorkspaceInWSM", ctx)(_ =>
        Future(workspaceManagerDAO.deleteWorkspaceV2(workspaceId, UUID.randomUUID().toString, ctx))
      )
      deletionJobId = deletionJobResult.getJobReport.getId
      _ <- traceWithParent("pollWorkspaceDeletionInWSM", ctx) { _ =>
        pollWMOperation(workspaceId,
                        deletionJobId,
                        ctx,
                        2 seconds,
                        wsmConfig.deletionPollTimeout,
                        "Deletion",
                        getWorkspaceDeletionStatus
        )
      }
    } yield {}
  }

  private def getWorkspaceFromWsm(workspaceId: UUID, ctx: RawlsRequestContext): Option[WorkspaceDescription] =
    Try(workspaceManagerDAO.getWorkspace(workspaceId, ctx)) match {
      case Success(w) => Some(w)
      case Failure(e: ApiException) if e.getCode == 404 =>
        logger.warn(s"Workspace not found in workspace manager for deletion [id=${workspaceId}]")
        None
      case Failure(e) =>
        throw new RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.InternalServerError, e)
        )
    }

  private def getWorkspaceDeletionStatus(workspaceId: UUID,
                                         jobControlId: String,
                                         ctx: RawlsRequestContext
  ): Future[Option[JobResult]] = {
    val result: JobResult = Try(workspaceManagerDAO.getDeleteWorkspaceV2Result(workspaceId, jobControlId, ctx)) match {
      case Success(w) => w
      case Failure(e: ApiException) =>
        if (e.getCode == StatusCodes.Forbidden.intValue) {
          // WSM will give back a 403 during polling if the workspace is not present or already deleted.
          logger.info(
            s"Workspace deletion result status = ${e.getCode} for workspace ID ${workspaceId}, WSM record is gone. Proceeding with rawls workspace deletion"
          )
          return Future.successful(None)
        } else {
          throw e
        }
      case Failure(e) => throw e
    }

    result.getJobReport.getStatus match {
      case StatusEnum.SUCCEEDED => Future.successful(Some(result))
      case _ =>
        Future.failed(
          new WorkspaceManagerPollingOperationException(
            s"Polling workspace deletion [workspaceId=${workspaceId}, jobControlId = ${jobControlId}] for status to be ${StatusEnum.SUCCEEDED}. Current status: ${result.getJobReport.getStatus}.",
            result.getJobReport.getStatus
          )
        )
    }
  }

  private def createWorkspace(workspaceRequest: WorkspaceRequest,
                              profile: ProfileModel,
                              parentContext: RawlsRequestContext
  ): Future[Workspace] = {
    val wsmConfig = multiCloudWorkspaceConfig.workspaceManager
      .getOrElse(throw new RawlsException("WSM app config not present"))

    val spendProfileId = profile.getId.toString
    val workspaceId = UUID.randomUUID()
    (for {
      _ <- requireCreateWorkspaceAction(RawlsBillingProjectName(workspaceRequest.namespace))

      _ = logger.info(s"Creating workspace record [workspaceId = ${workspaceId}]")
      savedWorkspace <- traceWithParent("saveMultiCloudWorkspaceToDB", parentContext)(_ =>
        createNewWorkspaceRecord(workspaceId, workspaceRequest, parentContext)
      )

      _ = logger.info(s"Creating workspace in WSM [workspaceId = ${workspaceId}]")
      _ <- traceWithParent("createMultiCloudWorkspaceInWSM", parentContext) { _ =>
        Future(
          workspaceManagerDAO.createWorkspaceWithSpendProfile(
            workspaceId,
            workspaceRequest.name,
            spendProfileId,
            workspaceRequest.namespace,
            Seq(wsmConfig.leonardoWsmApplicationId),
            buildPolicyInputs(workspaceRequest),
            ctx
          )
        )
      }
      _ = logger.info(s"Creating cloud context in WSM [workspaceId = ${workspaceId}]")
      cloudContextCreateResult <- traceWithParent("createAzureCloudContextInWSM", parentContext)(_ =>
        Future(
          workspaceManagerDAO.createAzureWorkspaceCloudContext(workspaceId, ctx)
        )
      )
      jobControlId = cloudContextCreateResult.getJobReport.getId
      _ = logger.info(s"Polling on cloud context in WSM [workspaceId = ${workspaceId}, jobControlId = ${jobControlId}]")
      _ <- traceWithParent("pollGetCloudContextCreationStatusInWSM", parentContext)(_ =>
        pollWMOperation(workspaceId,
                        cloudContextCreateResult.getJobReport.getId,
                        ctx,
                        2 seconds,
                        wsmConfig.pollTimeout,
                        "Cloud context",
                        getCloudContextCreationStatus
        )
      )

      containerResult <- traceWithParent("createStorageContainer", parentContext)(_ =>
        Future(
          workspaceManagerDAO.createAzureStorageContainer(
            workspaceId,
            MultiCloudWorkspaceService.getStorageContainerName(workspaceId),
            ctx
          )
        )
      )
      _ = logger.info(
        s"Created Azure storage container in WSM [workspaceId = ${workspaceId}, containerId = ${containerResult.getResourceId}]"
      )

      // create a WDS application in Leo
      _ <- createWdsAppInWorkspace(workspaceId, parentContext, None, workspaceRequest.attributes)

    } yield savedWorkspace).recoverWith { case e @ (_: ApiException | _: WorkspaceManagerOperationFailureException) =>
      logger.info(s"Error creating workspace ${workspaceRequest.toWorkspaceName} [workspaceId = ${workspaceId}]", e)
      for {
        _ <- deleteWorkspaceInWSM(workspaceId).recover { case e =>
          logger.info(
            s"Error cleaning up workspace ${workspaceRequest.toWorkspaceName} in WSM [workspaceId = ${workspaceId}",
            e
          )
        }
        _ <- dataSource.inTransaction(_.workspaceQuery.delete(workspaceRequest.toWorkspaceName))
      } yield throw new RawlsExceptionWithErrorReport(
        ErrorReport(StatusCodes.InternalServerError,
                    s"Error creating workspace ${workspaceRequest.toWorkspaceName}. Please try again."
        )
      )
    }
  }

  private def buildPolicyInputs(workspaceRequest: WorkspaceRequest) =
    workspaceRequest.protectedData match {
      case Some(true) =>
        Some(
          new WsmPolicyInputs()
            .inputs(
              Seq(
                new WsmPolicyInput()
                  .name("protected-data")
                  .namespace("terra")
                  .additionalData(List().asJava)
              ).asJava
            )
        )
      case _ => None
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

  private def getWorkspaceCloneStatus(workspaceId: UUID,
                                      jobControlId: String,
                                      localCtx: RawlsRequestContext
  ): Future[CloneWorkspaceResult] = {
    val result = workspaceManagerDAO.getCloneWorkspaceResult(workspaceId, jobControlId, localCtx)
    result.getJobReport.getStatus match {
      case StatusEnum.SUCCEEDED => Future.successful(result)
      case _ =>
        Future.failed(
          new WorkspaceManagerPollingOperationException(
            s"Polling workspace clone operation [jobControlId = ${jobControlId}] for status to be ${StatusEnum.SUCCEEDED}. Current status: ${result.getJobReport.getStatus}.",
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
  private def pollWMOperation(workspaceId: UUID,
                              jobControlId: String,
                              localCtx: RawlsRequestContext,
                              interval: FiniteDuration,
                              pollTimeout: FiniteDuration,
                              resourceType: String,
                              getOperationStatus: (UUID, String, RawlsRequestContext) => Future[Object]
  ): Future[Unit] =
    for {
      result <- retryUntilSuccessOrTimeout(pred = jobStatusPredicate)(interval, pollTimeout) { () =>
        getOperationStatus(workspaceId, jobControlId, localCtx)
      }
    } yield result match {
      case Left(_) =>
        throw new WorkspaceManagerOperationFailureException(
          s"${resourceType} failed [workspaceId=${workspaceId}, jobControlId=${jobControlId}]",
          workspaceId,
          jobControlId
        )
      case Right(_) => ()
    }

  private def deleteWorkspaceRecord(workspace: Workspace) =
    dataSource.inTransaction { access =>
      access.workspaceQuery.delete(workspace.toWorkspaceName)
    }

  private def createNewWorkspaceRecord(workspaceId: UUID,
                                       request: WorkspaceRequest,
                                       parentContext: RawlsRequestContext
  ): Future[Workspace] =
    dataSource.inTransaction { access =>
      for {
        _ <- failIfWorkspaceExists(request.toWorkspaceName)
        newWorkspace <- createMultiCloudWorkspaceInDatabase(
          workspaceId.toString,
          request.toWorkspaceName,
          request.attributes,
          access,
          parentContext
        )
      } yield newWorkspace
    }

  private def createMultiCloudWorkspaceInDatabase(workspaceId: String,
                                                  workspaceName: WorkspaceName,
                                                  attributes: AttributeMap,
                                                  dataAccess: DataAccess,
                                                  parentContext: RawlsRequestContext
  ): ReadWriteAction[Workspace] = {
    val currentDate = DateTime.now
    val workspace = Workspace.buildMcWorkspace(
      namespace = workspaceName.namespace,
      name = workspaceName.name,
      workspaceId = workspaceId,
      createdDate = currentDate,
      lastModified = currentDate,
      createdBy = ctx.userInfo.userEmail.value,
      attributes = attributes,
      WorkspaceState.Ready
    )
    traceDBIOWithParent("saveMultiCloudWorkspace", parentContext)(_ =>
      dataAccess.workspaceQuery.createOrUpdate(workspace)
    )
  }
  private def createWdsAppInWorkspace(workspaceId: UUID,
                                      parentContext: RawlsRequestContext,
                                      sourceWorkspaceId: Option[UUID],
                                      workspaceAttributeMap: AttributeMap
  ): Future[Unit] =
    workspaceAttributeMap.get(AttributeName.withDefaultNS("disableAutomaticAppCreation")) match {
      case Some(AttributeString("true")) | Some(AttributeBoolean(true)) =>
        // Skip WDS deployment for testing purposes.
        logger.info("Skipping creation of WDS per request attributes")
        Future.successful()
      case _ =>
        // create a WDS application in Leo. Do not fail workspace creation if WDS creation fails.
        logger.info(s"Creating WDS instance [workspaceId = ${workspaceId}]")
        traceWithParent("createWDSInstance", parentContext)(_ =>
          Future(
            leonardoDAO.createWDSInstance(parentContext.userInfo.accessToken.token, workspaceId, sourceWorkspaceId)
          )
            .recover { case t: Throwable =>
              // fail silently, but log the error
              logger.error(s"Error creating WDS instance [workspaceId = ${workspaceId}]: ${t.getMessage}", t)
            }
        )
    }

}

class WorkspaceManagerOperationFailureException(message: String, val workspaceId: UUID, val jobControlId: String)
    extends RawlsException(message)

class WorkspaceManagerPollingOperationException(message: String, val status: StatusEnum) extends Exception(message)
