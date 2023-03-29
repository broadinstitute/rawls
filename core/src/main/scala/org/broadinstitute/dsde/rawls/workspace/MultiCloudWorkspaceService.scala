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
import org.broadinstitute.dsde.rawls.config.{LeonardoConfig, MultiCloudWorkspaceConfig}
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction, WorkspaceManagerResourceMonitorRecord}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{HttpLeonardoDAO, SamDAO, SlickDataSource, WorkspaceManagerResourceMonitorRecordDao}
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.WorkspaceType.{McWorkspace, RawlsWorkspace}
import org.broadinstitute.dsde.rawls.model.{ErrorReport, RawlsBillingProject, RawlsBillingProjectName, RawlsRequestContext, SamWorkspaceActions, Workspace, WorkspaceName, WorkspaceRequest}
import org.broadinstitute.dsde.rawls.util.TracingUtils.{traceDBIOWithParent, traceWithParent}
import org.broadinstitute.dsde.rawls.util.{Retry, WorkspaceSupport}
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.joda.time.DateTime

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.language.postfixOps
import scala.util.{Success, Try}

object MultiCloudWorkspaceService {
  def constructor(dataSource: SlickDataSource,
                  workspaceManagerDAO: WorkspaceManagerDAO,
                  billingProfileManagerDAO: BillingProfileManagerDAO,
                  samDAO: SamDAO,
                  multiCloudWorkspaceConfig: MultiCloudWorkspaceConfig,
                  leonardoConfig: LeonardoConfig,
                  workbenchMetricBaseName: String
  )(ctx: RawlsRequestContext)(implicit ec: ExecutionContext, system: ActorSystem): MultiCloudWorkspaceService =
    new MultiCloudWorkspaceService(
      ctx,
      workspaceManagerDAO,
      billingProfileManagerDAO,
      samDAO,
      multiCloudWorkspaceConfig,
      leonardoConfig,
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
                                 val leonardoConfig: LeonardoConfig,
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
    *
    * @param workspaceRequest Incoming workspace creation request
    * @param workspaceService Workspace service that will handle legacy creation requests
    * @param parentSpan       OpenCensus span
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
              context
            )
          })
        }
        jobControlId = cloneResult.getJobReport.getId
        _ = logger.info(
          s"Polling on workspace clone in WSM [workspaceId = ${workspaceId}, jobControlId = ${jobControlId}]"
        )
        _ <- traceWithParent("workspaceManagerDAO.getWorkspaceCloneStatus", parentContext) { context =>
          pollWMCreation(workspaceId,
                         jobControlId,
                         context,
                         2 seconds,
                         wsmConfig.pollTimeout,
                         "Clone workspace",
                         getWorkspaceCloneStatus
          )
        }
        _ <- traceWithParent("workspaceManagerDAO.disableLeo", parentContext) { context =>
          Future(blocking {
            workspaceManagerDAO.disableApplication(workspaceId, wsmConfig.leonardoWsmApplicationId, context)
          })
        }
        _ <- traceWithParent("workspaceManagerDAO.reenableLeo", parentContext) { context =>
          Future(blocking {
            workspaceManagerDAO.enableApplication(workspaceId, wsmConfig.leonardoWsmApplicationId, context)
          })
        }
        _ = logger.info(
          s"Starting workspace storage container clone in WSM [workspaceId = ${workspaceId}]"
        )
        containerCloneResult <- traceWithParent("workspaceManagerDAO.cloneAzureStorageContainer", parentContext) {
          context =>
            cloneWorkspaceStorageContainer(sourceWorkspace.workspaceIdAsUUID, workspaceId, context)
        }
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
    * @param parentSpan       OpenCensus span
    * @return Future containing the created Workspace's information
    */
  def createMultiCloudWorkspace(workspaceRequest: WorkspaceRequest,
                                profile: ProfileModel,
                                parentContext: RawlsRequestContext = ctx
  ): Future[Workspace] = {
    if (!multiCloudWorkspaceConfig.multiCloudWorkspacesEnabled) {
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotImplemented, "MC workspaces are not enabled"))
    }

    traceWithParent("createMultiCloudWorkspace", parentContext)(s1 =>
      createWorkspace(workspaceRequest, profile, s1) andThen { case Success(_) =>
        createdMultiCloudWorkspaceCounter.inc()
      }
    )
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

      _ = logger.info(s"Creating workspace record [workspaceId = ${workspaceId}")
      savedWorkspace <- traceWithParent("saveMultiCloudWorkspaceToDB", parentContext)(_ =>
        createNewWorkspaceRecord(workspaceId, workspaceRequest, parentContext)
      )

      _ = logger.info(s"Creating workspace in WSM [workspaceId = ${workspaceId}]")
      _ <- traceWithParent("createMultiCloudWorkspaceInWSM", parentContext)(_ =>
        Future(
          workspaceManagerDAO.createWorkspaceWithSpendProfile(workspaceId, workspaceRequest.name, spendProfileId, ctx)
        )
      )
      _ = logger.info(s"Creating cloud context in WSM [workspaceId = ${workspaceId}]")
      cloudContextCreateResult <- traceWithParent("createAzureCloudContextInWSM", parentContext)(_ =>
        Future(
          workspaceManagerDAO.createAzureWorkspaceCloudContext(workspaceId, ctx)
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

      _ = logger.info(s"Enabling leonardo app in WSM [workspaceId = ${workspaceId}]")
      _ <- traceWithParent("enableLeoInWSM", parentContext)(_ =>
        Future(workspaceManagerDAO.enableApplication(workspaceId, wsmConfig.leonardoWsmApplicationId, ctx))
      )
      containerResult <- traceWithParent("createStorageContainer", parentContext)(_ =>
        Future(
          workspaceManagerDAO.createAzureStorageContainer(
            workspaceId,
            MultiCloudWorkspaceService.getStorageContainerName(workspaceId),
            None,
            ctx
          )
        )
      )

      _ <- traceWithParent("createWDSInstance", parentContext) { _ =>
        logger.info(s"Creating WDS instance - workspace:'${workspaceRequest.toWorkspaceName.name}' - UUID:${workspaceId}")
        val httpLeonardoDAO = new HttpLeonardoDAO(leonardoConfig)
        Future(httpLeonardoDAO.createWDSInstance(
          parentContext.userInfo.accessToken.token, workspaceRequest.name, s"wds-${workspaceId}", "CROMWELL")
        )
      }

      _ = logger.info(
        s"Created Azure storage container in WSM [workspaceId = ${workspaceId}, containerId = ${containerResult.getResourceId}]"
      )
    } yield savedWorkspace).recoverWith { case e @ (_: ApiException | _: WorkspaceManagerCreationFailureException) =>
      logger.info(s"Error creating workspace ${workspaceRequest.toWorkspaceName} [workspaceId = ${workspaceId}]", e)
      for {
        _ <- Future(blocking {
          workspaceManagerDAO.deleteWorkspace(workspaceId, ctx)
        }).recover { case e =>
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
      attributes = attributes
    )
    traceDBIOWithParent("saveMultiCloudWorkspace", parentContext)(_ =>
      dataAccess.workspaceQuery.createOrUpdate(workspace)
    )
  }
}

class WorkspaceManagerCreationFailureException(message: String, val workspaceId: UUID, val jobControlId: String)
    extends RawlsException(message)

class WorkspaceManagerPollingOperationException(message: String, val status: StatusEnum) extends Exception(message)
