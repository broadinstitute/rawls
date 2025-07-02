package org.broadinstitute.dsde.rawls.workspace

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import bio.terra.profile.model.ProfileModel
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model
import bio.terra.workspace.model.JobReport.StatusEnum
import bio.terra.workspace.model._
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.billing.{BillingProfileManagerDAO, BillingRepository}
import org.broadinstitute.dsde.rawls.config.MultiCloudWorkspaceConfig
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
import org.broadinstitute.dsde.rawls.model.{
  AttributeBoolean,
  AttributeName,
  AttributeString,
  ErrorReport,
  RawlsBillingProject,
  RawlsBillingProjectName,
  RawlsRequestContext,
  Workspace,
  WorkspaceRequest
}
import org.broadinstitute.dsde.rawls.util.TracingUtils.{traceFutureWithParent, traceNakedWithParent}
import org.broadinstitute.dsde.rawls.util.{BillingProjectSupport, Retry, WorkspaceSupport}
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}

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

}

class WorkspaceManagerOperationFailureException(message: String, val workspaceId: UUID, val jobControlId: String)
    extends RawlsException(message)

class WorkspaceManagerPollingOperationException(message: String, val status: StatusEnum) extends Exception(message)
