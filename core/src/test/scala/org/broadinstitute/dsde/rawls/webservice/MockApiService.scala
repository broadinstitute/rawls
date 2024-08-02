package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Directives.{provide, _}
import akka.http.scaladsl.server.Directive1
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import akka.http.scaladsl.server._
import akka.stream.Materializer
import io.opentelemetry.context.Context
import org.broadinstitute.dsde.rawls.billing._
import org.broadinstitute.dsde.rawls.bucketMigration.BucketMigrationService
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.entities.EntityService
import org.broadinstitute.dsde.rawls.genomics.GenomicsService
import org.broadinstitute.dsde.rawls.methods.MethodConfigurationService
import org.broadinstitute.dsde.rawls.model.{ApplicationVersion, RawlsRequestContext, RawlsUserEmail, RawlsUserSubjectId, UserInfo}
import org.broadinstitute.dsde.rawls.snapshot.SnapshotService
import org.broadinstitute.dsde.rawls.spendreporting.SpendReportingService
import org.broadinstitute.dsde.rawls.status.StatusService
import org.broadinstitute.dsde.rawls.submissions.SubmissionsService
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.workspace.{MultiCloudWorkspaceService, WorkspaceService}
import org.broadinstitute.dsde.workbench.oauth2.OpenIDConnectConfiguration
import org.scalatestplus.mockito.MockitoSugar.mock

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import scala.language.postfixOps


class MockApiService(
                      val userInfo: UserInfo = UserInfo(RawlsUserEmail(""), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212349")),
                      override val openIDConnectConfiguration: OpenIDConnectConfiguration = mock[OpenIDConnectConfiguration],
                      override val samDAO: SamDAO = mock[SamDAO],
                      override val submissionTimeout: FiniteDuration = mock[FiniteDuration],
                      override val workbenchMetricBaseName: String = "workbenchMetricBaseName-test",
                      override val batchUpsertMaxBytes: Long = 1028,
                      override val executionServiceCluster: ExecutionServiceCluster = mock[ExecutionServiceCluster],
                      override val appVersion: ApplicationVersion = mock[ApplicationVersion],
                      override val spendReportingConstructor: RawlsRequestContext => SpendReportingService = _ =>
                        mock[SpendReportingService],
                      override val billingProjectOrchestratorConstructor: RawlsRequestContext => BillingProjectOrchestrator = _ =>
                        mock[BillingProjectOrchestrator],
                      override val entityServiceConstructor: RawlsRequestContext => EntityService = _ => mock[EntityService],
                      override val genomicsServiceConstructor: RawlsRequestContext => GenomicsService = _ => mock[GenomicsService],
                      override val snapshotServiceConstructor: RawlsRequestContext => SnapshotService = _ => mock[SnapshotService],
                      override val statusServiceConstructor: () => StatusService = () => mock[StatusService],
                      override val methodConfigurationServiceConstructor: RawlsRequestContext => MethodConfigurationService = _ =>
                        mock[MethodConfigurationService],
                      override val submissionsServiceConstructor: RawlsRequestContext => SubmissionsService = _ => mock[SubmissionsService],
                      override val workspaceServiceConstructor: RawlsRequestContext => WorkspaceService = _ => mock[WorkspaceService],
                      override val multiCloudWorkspaceServiceConstructor: RawlsRequestContext => MultiCloudWorkspaceService = _ =>
                        mock[MultiCloudWorkspaceService],
                      override val bucketMigrationServiceConstructor: RawlsRequestContext => BucketMigrationService = _ =>
                        mock[BucketMigrationService],
                      override val userServiceConstructor: RawlsRequestContext => UserService = _ => mock[UserService]
                    )(implicit val executionContext: ExecutionContext, override val materializer: Materializer)
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


  override def requireUserInfo(otelContext: Option[Context]): Directive1[UserInfo] = provide(userInfo)
  
}
