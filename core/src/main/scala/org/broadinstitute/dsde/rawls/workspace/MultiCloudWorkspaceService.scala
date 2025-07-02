package org.broadinstitute.dsde.rawls.workspace

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import bio.terra.profile.model.{CloudPlatform, ProfileModel}
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model
import bio.terra.workspace.model.JobReport.StatusEnum
import bio.terra.workspace.model._
import cats.Apply
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.billing.{BillingProfileManagerDAO, BillingRepository}
import org.broadinstitute.dsde.rawls.config.MultiCloudWorkspaceConfig
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{
  LeonardoDAO,
  SamDAO,
  SlickDataSource,
  WorkspaceManagerResourceMonitorRecordDao
}
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.TpsModel.{TERRA_POLICY_NAMESPACE, TpsPolicies}
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
  WorkspaceCloudPlatform,
  WorkspaceDeletionResult,
  WorkspaceDetails,
  WorkspaceName,
  WorkspaceRequest,
  WorkspaceState,
  WorkspaceType
}
import org.broadinstitute.dsde.rawls.monitor.workspace.runners.clone.WorkspaceCloningRunner
import org.broadinstitute.dsde.rawls.util.TracingUtils.{traceFutureWithParent, traceNakedWithParent}
import org.broadinstitute.dsde.rawls.util.{BillingProjectSupport, Retry, WorkspaceSupport}
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.joda.time.{DateTime, DateTimeZone}

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{blocking, ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

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
      workbenchMetricBaseName,
      WorkspaceManagerResourceMonitorRecordDao(dataSource),
      new WorkspaceRepository(dataSource),
      new BillingRepository(dataSource)
    )

  def getStorageContainerName(workspaceId: UUID): String = s"sc-${workspaceId}"

  def buildPolicyInputs(workspaceRequest: WorkspaceRequest): Option[WsmPolicyInputs] = {
    val synthesizedProtectedDataPolicyInput: Option[Seq[WsmPolicyInput]] = workspaceRequest.protectedData match {
      case Some(true) =>
        Some(
          Seq(
            new WsmPolicyInput()
              .name(TpsPolicies.ProtectedData.name)
              .namespace(TERRA_POLICY_NAMESPACE)
              .additionalData(List().asJava)
          )
        )
      case _ => None
    }

    val otherPolicyInputs: Option[Seq[WsmPolicyInput]] = workspaceRequest.policies match {
      case Some(inputs) =>
        Some(inputs.map { requestedPolicy =>
          requestedPolicy.toWsmPolicyInput()
        })
      case _ => None
    }

    val merged: Option[WsmPolicyInputs] = synthesizedProtectedDataPolicyInput |+| otherPolicyInputs match {
      case Some(mergedInputs) => Some(new WsmPolicyInputs().inputs(mergedInputs.asJava))
      case _                  => None
    }

    merged
  }
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
                                 override val workbenchMetricBaseName: String,
                                 workspaceManagerResourceMonitorRecordDao: WorkspaceManagerResourceMonitorRecordDao,
                                 val workspaceRepository: WorkspaceRepository,
                                 val billingRepository: BillingRepository
)(implicit override val executionContext: ExecutionContext, val system: ActorSystem)
    extends LazyLogging
    with RawlsInstrumented
    with Retry
    with WorkspaceSupport
    with BillingProjectSupport {

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
        profileModel <- traceFutureWithParent("getBillingProfile", parentContext) { s =>
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
  ): Future[WorkspaceDetails] =
    for {
      sourceWs <- getV2WorkspaceContextAndPermissions(sourceWorkspaceName, SamWorkspaceActions.read)
      billingProject <- getBillingProjectContext(RawlsBillingProjectName(destWorkspaceRequest.namespace))
      _ <- requireCreateWorkspaceAction(billingProject.projectName)
      billingProfileOpt <- getBillingProfile(billingProject)
      (clone, cloudPlatform) <- (sourceWs.workspaceType, billingProfileOpt) match {

        case (McWorkspace, Some(profile)) if profile.getCloudPlatform == CloudPlatform.AZURE =>
          traceFutureWithParent("cloneAzureWorkspace", ctx) { s =>
            cloneAzureWorkspace(sourceWs, profile, destWorkspaceRequest, s).map(workspace =>
              (workspace, WorkspaceCloudPlatform.Azure)
            )
          }

        case (RawlsWorkspace, profileOpt)
            if profileOpt.isEmpty ||
              profileOpt.map(_.getCloudPlatform).contains(CloudPlatform.GCP) =>
          traceFutureWithParent("cloneRawlsWorkspace", ctx) { s =>
            wsService
              .cloneWorkspace(sourceWorkspaceName, destWorkspaceRequest, s)
              .map(workspace => (workspace, WorkspaceCloudPlatform.Gcp))
          }

        case (wsType, profileOpt) =>
          Future.failed(
            RawlsExceptionWithErrorReport(
              ErrorReport(
                StatusCodes.BadRequest,
                s"Cloud platform mismatch: Cannot clone $wsType workspace '$sourceWorkspaceName' " +
                  s"into billing project '${billingProject.projectName}' " +
                  s"(hosted on ${profileOpt.map(_.getCloudPlatform).getOrElse("Unknown")})."
              )
            )
          )
      }
    } yield WorkspaceDetails.fromWorkspaceAndOptions(
      clone,
      Some(destWorkspaceRequest.authorizationDomain.getOrElse(Set.empty)),
      useAttributes = true,
      Some(cloudPlatform)
    )

  def cloneAzureWorkspace(sourceWorkspace: Workspace,
                          profile: ProfileModel,
                          request: WorkspaceRequest,
                          parentContext: RawlsRequestContext
  ): Future[Workspace] = {

    assertBillingProfileCreationDate(profile)
    validateWorkspaceRequest(request)

    val wsmConfig = multiCloudWorkspaceConfig.workspaceManager
    val workspaceId = UUID.randomUUID()

    // Merge together source workspace and destination request attributes
    val mergedAttributes = sourceWorkspace.attributes ++ request.attributes
    for {
      // The call to WSM is asynchronous. Before we fire it off, allocate a new workspace record
      // to avoid naming conflicts - we'll erase it should the clone request to WSM fail.
      newWorkspace <- workspaceRepository.createMCWorkspace(
        workspaceId,
        request.toWorkspaceName,
        mergedAttributes,
        parentContext
      )
      containerCloneResult <- (for {
        cloneResult <- traceFutureWithParent("workspaceManagerDAO.cloneWorkspace", parentContext) { context =>
          Future(blocking {
            workspaceManagerDAO.cloneWorkspace(
              sourceWorkspaceId = sourceWorkspace.workspaceIdAsUUID,
              workspaceId = workspaceId,
              displayName = request.name,
              spendProfile = Option(profile),
              billingProjectNamespace = request.namespace,
              context,
              MultiCloudWorkspaceService.buildPolicyInputs(request)
            )
          })
        }
        jobControlId = cloneResult.getJobReport.getId
        _ = logger.info(
          s"Polling on workspace clone in WSM [workspaceId = ${workspaceId}, jobControlId = ${jobControlId}]"
        )
        _ <- traceFutureWithParent("workspaceManagerDAO.getWorkspaceCloneStatus", parentContext) { context =>
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
        containerCloneResult <- traceFutureWithParent("workspaceManagerDAO.cloneAzureStorageContainer", parentContext) {
          context =>
            cloneWorkspaceStorageContainer(sourceWorkspace.workspaceIdAsUUID,
                                           workspaceId,
                                           request.copyFilesWithPrefix,
                                           context
            )
        }

        // create a WDS application in Leo
        _ = createWdsAppInWorkspace(workspaceId,
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
        workspaceRepository.deleteWorkspace(newWorkspace) >> Future.failed(t)
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
      _ <- workspaceManagerResourceMonitorRecordDao.create(
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
    assertBillingProfileCreationDate(profile)
    validateWorkspaceRequest(workspaceRequest)

    traceFutureWithParent("createMultiCloudWorkspace", parentContext)(s1 =>
      createMultiCloudWorkspaceInt(workspaceRequest, UUID.randomUUID(), profile, s1) andThen { case Success(_) =>
        createdMultiCloudWorkspaceCounter.inc()
      }
    )
  }

  private def validateWorkspaceRequest(request: WorkspaceRequest): Unit =
    if (request.authorizationDomain.exists(_.nonEmpty)) {
      throw new RawlsExceptionWithErrorReport(
        ErrorReport(
          StatusCodes.BadRequest,
          "Azure workspaces do not support authorization domains. To limit workspace access to members of a set of Terra groups, use a group-constraint policy."
        )
      )
    }

  def assertBillingProfileCreationDate(profile: ProfileModel): Unit = {
    val previewDate = new DateTime(2023, 9, 12, 0, 0, DateTimeZone.UTC)
    val isTestProfile = profile.getCreatedDate == null || profile.getCreatedDate == ""
    if (!isTestProfile && DateTime.parse(profile.getCreatedDate).isBefore(previewDate)) {
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
    def getWorkspaceFromWsm(workspaceId: UUID, ctx: RawlsRequestContext): Option[WorkspaceDescription] =
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

    val wsmConfig = multiCloudWorkspaceConfig.workspaceManager
    getWorkspaceFromWsm(workspaceId, ctx).getOrElse(return Future.successful())
    for {
      // kick off the deletion job w/WSM
      deletionJobResult <- traceFutureWithParent("deleteWorkspaceInWSM", ctx)(_ =>
        Future(workspaceManagerDAO.deleteWorkspaceV2(workspaceId, UUID.randomUUID().toString, ctx))
      )
      deletionJobId = deletionJobResult.getJobReport.getId
      _ <- traceFutureWithParent("pollWorkspaceDeletionInWSM", ctx) { _ =>
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

  // visible so it can be called directly for testing
  def createMultiCloudWorkspaceInt(workspaceRequest: WorkspaceRequest,
                                   workspaceId: UUID,
                                   profile: ProfileModel,
                                   parentContext: RawlsRequestContext
  ): Future[Workspace] = {
    val wsmConfig = multiCloudWorkspaceConfig.workspaceManager
    val spendProfileId = profile.getId.toString
    (for {
      _ <- requireCreateWorkspaceAction(RawlsBillingProjectName(workspaceRequest.namespace))
      _ = logger.info(s"Creating workspace record [workspaceId = ${workspaceId}]")
      savedWorkspace <- traceFutureWithParent("saveMultiCloudWorkspaceToDB", parentContext)(_ =>
        workspaceRepository.createMCWorkspace(
          workspaceId,
          workspaceRequest.toWorkspaceName,
          workspaceRequest.attributes,
          parentContext
        )
      )
      _ = logger.info(s"Creating workspace with cloud context in WSM [workspaceId = ${workspaceId}]")
      createWorkspaceResult <- traceFutureWithParent("createMultiCloudWorkspaceInWSM", parentContext) { _ =>
        Future(
          workspaceManagerDAO.createWorkspaceWithSpendProfile(
            workspaceId,
            workspaceRequest.name,
            spendProfileId,
            workspaceRequest.namespace,
            Seq(wsmConfig.leonardoWsmApplicationId),
            model.CloudPlatform.AZURE,
            MultiCloudWorkspaceService.buildPolicyInputs(workspaceRequest),
            ctx
          )
        )
      }
      jobControlId = createWorkspaceResult.getJobReport.getId
      _ = logger.info(
        s"Polling on workspace creation in WSM [workspaceId = ${workspaceId}, jobControlId = ${jobControlId}]"
      )
      _ <- traceFutureWithParent("pollWorkspaceCreationStatusInWSM", parentContext)(_ =>
        pollWMOperation(workspaceId,
                        createWorkspaceResult.getJobReport.getId,
                        ctx,
                        2 seconds,
                        wsmConfig.pollTimeout,
                        "Workspace Creation",
                        getWorkspaceCreationStatus
        )
      )

      containerResult <- traceFutureWithParent("createStorageContainer", parentContext)(_ =>
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
      _ = createWdsAppInWorkspace(workspaceId, parentContext, None, workspaceRequest.attributes)

    } yield savedWorkspace).recoverWith {
      case r: RawlsExceptionWithErrorReport if r.errorReport.statusCode.contains(StatusCodes.Conflict) =>
        // Workspace already exists with this name/namespace, and we don't want to delete it.
        Future.failed(r)
      case e @ (_: ApiException | _: WorkspaceManagerOperationFailureException | _: RawlsExceptionWithErrorReport) =>
        logger.info(s"Error creating workspace ${workspaceRequest.toWorkspaceName} [workspaceId = ${workspaceId}]", e)
        for {
          _ <- deleteWorkspaceInWSM(workspaceId).recover { case e =>
            logger.info(
              s"Error cleaning up workspace ${workspaceRequest.toWorkspaceName} in WSM [workspaceId = ${workspaceId}",
              e
            )
          }
          _ <- workspaceRepository.deleteWorkspace(workspaceRequest.toWorkspaceName)
        } yield e match {
          case rawlsException: RawlsExceptionWithErrorReport => throw rawlsException
          case _ =>
            throw new RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.InternalServerError,
                          s"Error creating workspace ${workspaceRequest.toWorkspaceName}. Please try again."
              )
            )
        }
    }
  }

  private def getWorkspaceCreationStatus(_workspaceId: UUID, // Unused, but polling helper method passes it.
                                         jobControlId: String,
                                         localCtx: RawlsRequestContext
  ): Future[CreateWorkspaceV2Result] = {
    val result = workspaceManagerDAO.getCreateWorkspaceResult(jobControlId, localCtx)
    result.getJobReport.getStatus match {
      case StatusEnum.SUCCEEDED => Future.successful(result)
      case _ =>
        Future.failed(
          new WorkspaceManagerPollingOperationException(
            s"Polling workspace creation for [jobControlId = ${jobControlId}] for status to be ${StatusEnum.SUCCEEDED}. Current status: ${result.getJobReport.getStatus}.",
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

  private def pollWMOperation(workspaceId: UUID,
                              jobControlId: String,
                              localCtx: RawlsRequestContext,
                              interval: FiniteDuration,
                              pollTimeout: FiniteDuration,
                              resourceType: String,
                              getOperationStatus: (UUID, String, RawlsRequestContext) => Future[Object]
  ): Future[Unit] = {
    def jobStatusPredicate(t: Throwable): Boolean =
      t match {
        case t: WorkspaceManagerPollingOperationException => t.status == StatusEnum.RUNNING
        case _                                            => false
      }
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
  }

  private def createWdsAppInWorkspace(workspaceId: UUID,
                                      parentContext: RawlsRequestContext,
                                      sourceWorkspaceId: Option[UUID],
                                      workspaceAttributeMap: AttributeMap
  ): Unit =
    workspaceAttributeMap.get(AttributeName.withDefaultNS("disableAutomaticAppCreation")) match {
      case Some(AttributeString("true")) | Some(AttributeBoolean(true)) =>
        // Skip WDS deployment for testing purposes.
        logger.info("Skipping creation of WDS per request attributes")
      case _ =>
        // create a WDS application in Leo. Do not fail workspace creation if WDS creation fails.
        logger.info(s"Creating WDS instance [workspaceId = ${workspaceId}]")
        traceNakedWithParent("createWDSInstance", parentContext.toTracingContext)(_ =>
          Try(
            leonardoDAO.createWDSInstance(parentContext.userInfo.accessToken.token, workspaceId, sourceWorkspaceId)
          ).recover { case t: Throwable =>
            // fail silently, but log the error
            logger.error(s"Error creating WDS instance [workspaceId = ${workspaceId}]: ${t.getMessage}", t)
          }.get
        )
    }

}

class WorkspaceManagerOperationFailureException(message: String, val workspaceId: UUID, val jobControlId: String)
    extends RawlsException(message)

class WorkspaceManagerPollingOperationException(message: String, val status: StatusEnum) extends Exception(message)
