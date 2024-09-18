package org.broadinstitute.dsde.rawls.webservice

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives.{provide, _}
import akka.http.scaladsl.server.Directive1
import akka.stream.Materializer
import io.opentelemetry.context.Context
import org.broadinstitute.dsde.rawls.billing._
import org.broadinstitute.dsde.rawls.bucketMigration.BucketMigrationService
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.entities.EntityService
import org.broadinstitute.dsde.rawls.genomics.GenomicsService
import org.broadinstitute.dsde.rawls.methods.MethodConfigurationService
import org.broadinstitute.dsde.rawls.model.{
  ApplicationVersion,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  UserInfo
}
import org.broadinstitute.dsde.rawls.snapshot.SnapshotService
import org.broadinstitute.dsde.rawls.spendreporting.SpendReportingService
import org.broadinstitute.dsde.rawls.status.StatusService
import org.broadinstitute.dsde.rawls.submissions.SubmissionsService
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.workspace.{MultiCloudWorkspaceService, WorkspaceService, WorkspaceSettingService}
import org.broadinstitute.dsde.workbench.oauth2.OpenIDConnectConfiguration
import org.mockito.Mockito.RETURNS_SMART_NULLS
import org.scalatestplus.mockito.MockitoSugar.mock

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import scala.language.postfixOps

class MockApiService(
  val userInfo: UserInfo =
    UserInfo(RawlsUserEmail(""), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212349")),
  override val openIDConnectConfiguration: OpenIDConnectConfiguration =
    mock[OpenIDConnectConfiguration](RETURNS_SMART_NULLS),
  override val samDAO: SamDAO = mock[SamDAO](RETURNS_SMART_NULLS),
  override val submissionTimeout: FiniteDuration = mock[FiniteDuration](RETURNS_SMART_NULLS),
  override val workbenchMetricBaseName: String = "workbenchMetricBaseName-test",
  override val batchUpsertMaxBytes: Long = 1028,
  override val executionServiceCluster: ExecutionServiceCluster = mock[ExecutionServiceCluster](RETURNS_SMART_NULLS),
  override val appVersion: ApplicationVersion = mock[ApplicationVersion](RETURNS_SMART_NULLS),
  override val spendReportingConstructor: RawlsRequestContext => SpendReportingService = _ =>
    mock[SpendReportingService](RETURNS_SMART_NULLS),
  override val billingProjectOrchestratorConstructor: RawlsRequestContext => BillingProjectOrchestrator = _ =>
    mock[BillingProjectOrchestrator](RETURNS_SMART_NULLS),
  override val entityServiceConstructor: RawlsRequestContext => EntityService = _ =>
    mock[EntityService](RETURNS_SMART_NULLS),
  override val genomicsServiceConstructor: RawlsRequestContext => GenomicsService = _ =>
    mock[GenomicsService](RETURNS_SMART_NULLS),
  override val snapshotServiceConstructor: RawlsRequestContext => SnapshotService = _ =>
    mock[SnapshotService](RETURNS_SMART_NULLS),
  override val statusServiceConstructor: () => StatusService = () => mock[StatusService](RETURNS_SMART_NULLS),
  override val methodConfigurationServiceConstructor: RawlsRequestContext => MethodConfigurationService = _ =>
    mock[MethodConfigurationService](RETURNS_SMART_NULLS),
  override val submissionsServiceConstructor: RawlsRequestContext => SubmissionsService = _ =>
    mock[SubmissionsService](RETURNS_SMART_NULLS),
  override val workspaceServiceConstructor: RawlsRequestContext => WorkspaceService = _ =>
    mock[WorkspaceService](RETURNS_SMART_NULLS),
  override val multiCloudWorkspaceServiceConstructor: RawlsRequestContext => MultiCloudWorkspaceService = _ =>
    mock[MultiCloudWorkspaceService](RETURNS_SMART_NULLS),
  override val workspaceSettingServiceConstructor: RawlsRequestContext => WorkspaceSettingService = _ =>
    mock[WorkspaceSettingService](RETURNS_SMART_NULLS),
  override val bucketMigrationServiceConstructor: RawlsRequestContext => BucketMigrationService = _ =>
    mock[BucketMigrationService](RETURNS_SMART_NULLS),
  override val userServiceConstructor: RawlsRequestContext => UserService = _ => mock[UserService](RETURNS_SMART_NULLS)
)(implicit val executionContext: ExecutionContext)
    extends RawlsApiService
    with AdminApiService
    with BillingApiService
    with BillingApiServiceV2
    with EntityApiService
    with NotificationsApiService
    with SnapshotApiService
    with StatusApiService
    with UserApiService
    with MethodConfigApiService
    with WorkspaceApiService
    with SubmissionApiService {

  implicit val system: ActorSystem = ActorSystem("rawls")

  implicit override val materializer: Materializer = Materializer(system)

  override def requireUserInfo(otelContext: Option[Context]): Directive1[UserInfo] = provide(userInfo)

  def testRoutes: server.Route =
    (handleExceptions(RawlsApiService.exceptionHandler) & handleRejections(RawlsApiService.rejectionHandler)) {
      baseApiRoutes(Context.root())
    }

}
