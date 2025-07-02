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
    with BillingProjectSupport {}
