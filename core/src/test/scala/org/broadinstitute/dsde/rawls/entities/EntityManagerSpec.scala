package org.broadinstitute.dsde.rawls.entities

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.config.DataRepoEntityProviderConfig
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.DataRepoDAO
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleBigQueryServiceFactory, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.entities.local.LocalEntityProvider
import org.broadinstitute.dsde.rawls.entities.quicksilver.QuicksilverEntityProvider
import org.broadinstitute.dsde.rawls.model.WorkspaceSettingConfig.QuicksilverDataTablesConfig
import org.broadinstitute.dsde.rawls.model.WorkspaceSettingTypes.QuicksilverDataTables
import org.broadinstitute.dsde.rawls.model.{
  QuicksilverDataTablesSetting,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  UserInfo,
  Workspace
}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.workspace.WorkspaceSettingRepository
import org.joda.time.DateTime
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class EntityManagerSpec extends AnyFlatSpec with MockitoTestUtils with Matchers {

  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  val defaultRequestContext: RawlsRequestContext =
    RawlsRequestContext(
      UserInfo(RawlsUserEmail("test"), OAuth2BearerToken("Bearer 123"), 123, RawlsUserSubjectId("abc"))
    )

  val workspace: Workspace = Workspace(
    "entityManagerSpecTestWorkspace",
    "entityManagerSpecTestNamespace",
    UUID.randomUUID.toString,
    "bucketName",
    Some("workflowCollection"),
    new DateTime(),
    new DateTime(),
    "creator",
    Map.empty
  )

  "quicksilver data tables setting" should "control the provider returned by the EntityManager" in {
    val workspaceId = workspace.workspaceIdAsUUID

    val workspaceSettingRepository = mock[WorkspaceSettingRepository]
    val samDAO = mock[SamDAO]
    val mockDataSource = mock[SlickDataSource]
    val mockWorkspaceManagerDAO = mock[WorkspaceManagerDAO]
    val mockDataRepoDAO = mock[DataRepoDAO]
    val mockGoogleBigQueryServiceFactory = mock[GoogleBigQueryServiceFactory]
    val mockDataRepoEntityProviderConfig = mock[DataRepoEntityProviderConfig]

    val entityManager: EntityManager = EntityManager.defaultEntityManager(
      mockDataSource,
      mockWorkspaceManagerDAO,
      workspaceSettingRepository,
      mockDataRepoDAO,
      samDAO,
      mockGoogleBigQueryServiceFactory,
      mockDataRepoEntityProviderConfig,
      cacheEnabled = false,
      queryTimeout = java.time.Duration.ofSeconds(1),
      metricsPrefix = "metrics-prefix"
    )

    val entityRequestArguments = EntityRequestArguments(workspace, defaultRequestContext)

    // check the EntityManager behavior when the quicksilver setting is not set
    when(workspaceSettingRepository.getWorkspaceSettingOfType(workspaceId, QuicksilverDataTables))
      .thenReturn(Future.successful(None))
    val beforeSetting = Await.result(entityManager.resolveProviderFuture(entityRequestArguments), Duration.Inf)
    beforeSetting shouldBe a[LocalEntityProvider]

    // check the EntityManager behavior when quicksilver is enabled
    when(workspaceSettingRepository.getWorkspaceSettingOfType(workspaceId, QuicksilverDataTables))
      .thenReturn(Future.successful(Option(QuicksilverDataTablesSetting(QuicksilverDataTablesConfig(enabled = true)))))
    val afterSetting = Await.result(entityManager.resolveProviderFuture(entityRequestArguments), Duration.Inf)
    afterSetting shouldBe a[QuicksilverEntityProvider]

    // check the EntityManager behavior when quicksilver is disabled
    when(workspaceSettingRepository.getWorkspaceSettingOfType(workspaceId, QuicksilverDataTables))
      .thenReturn(Future.successful(Option(QuicksilverDataTablesSetting(QuicksilverDataTablesConfig(enabled = false)))))
    val afterUpdate = Await.result(entityManager.resolveProviderFuture(entityRequestArguments), Duration.Inf)
    afterUpdate shouldBe a[LocalEntityProvider]
  }

}
