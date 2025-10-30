package org.broadinstitute.dsde.rawls.entities

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.entities.base.AuditLoggingEntityProvider
import org.broadinstitute.dsde.rawls.entities.compact.CompactEntityProvider
import org.broadinstitute.dsde.rawls.entities.exceptions.DataEntityException
import org.broadinstitute.dsde.rawls.entities.local.LocalEntityProvider
import org.broadinstitute.dsde.rawls.model.WorkspaceSettingConfig.CompactDataTablesConfig
import org.broadinstitute.dsde.rawls.model.WorkspaceSettingTypes.CompactDataTables
import org.broadinstitute.dsde.rawls.model.{
  CompactDataTablesSetting,
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
  implicit private val system: ActorSystem = ActorSystem("EntityManagerSpec")

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

  "compact data tables setting" should "control the provider returned by the EntityManager" in {
    val workspaceId = workspace.workspaceIdAsUUID

    val workspaceSettingRepository = mock[WorkspaceSettingRepository]
    val mockDataSource = mock[SlickDataSource]

    val entityManager: EntityManager = EntityManager.defaultEntityManager(
      mockDataSource,
      metricsPrefix = "metrics-prefix"
    )

    val entityRequestArguments = EntityRequestArguments(workspace, defaultRequestContext)

    // Mock workspace has no pending compact data tables setting
    when(workspaceSettingRepository.hasPendingSettings(workspaceId, CompactDataTables))
      .thenReturn(Future.successful(false))

    // check the EntityManager behavior when the compact data tables setting is not set
    when(workspaceSettingRepository.getWorkspaceSettingOfType(workspaceId, CompactDataTables))
      .thenReturn(Future.successful(None))
    val beforeSetting = Await.result(entityManager.resolveProviderFuture(entityRequestArguments), Duration.Inf)

    // Provider should be an AuditLoggingEntityProvider with a LocalEntityProvider delegate
    beforeSetting shouldBe a[AuditLoggingEntityProvider]
    val beforeSettingAuditProvider = beforeSetting.asInstanceOf[AuditLoggingEntityProvider]
    beforeSettingAuditProvider.delegate shouldBe a[LocalEntityProvider]

    // check the EntityManager behavior when compact data tables is enabled
    when(workspaceSettingRepository.getWorkspaceSettingOfType(workspaceId, CompactDataTables))
      .thenReturn(Future.successful(Option(CompactDataTablesSetting(CompactDataTablesConfig(enabled = true)))))
    val afterSetting = Await.result(entityManager.resolveProviderFuture(entityRequestArguments), Duration.Inf)

    // Provider should be an AuditLoggingEntityProvider with a CompactEntityProvider delegate
    afterSetting shouldBe a[AuditLoggingEntityProvider]
    val afterSettingAuditProvider = afterSetting.asInstanceOf[AuditLoggingEntityProvider]
    afterSettingAuditProvider.delegate shouldBe a[CompactEntityProvider]

    // check the EntityManager behavior when compact data tables is disabled
    when(workspaceSettingRepository.getWorkspaceSettingOfType(workspaceId, CompactDataTables))
      .thenReturn(Future.successful(Option(CompactDataTablesSetting(CompactDataTablesConfig(enabled = false)))))
    val afterUpdate = Await.result(entityManager.resolveProviderFuture(entityRequestArguments), Duration.Inf)

    // Provider should be an AuditLoggingEntityProvider with a LocalEntityProvider delegate
    afterUpdate shouldBe a[AuditLoggingEntityProvider]
    val afterUpdateAuditProvider = afterUpdate.asInstanceOf[AuditLoggingEntityProvider]
    afterUpdateAuditProvider.delegate shouldBe a[LocalEntityProvider]

    // check the EntityManager behavior when there are pending compact data tables settings
    when(workspaceSettingRepository.hasPendingSettings(workspaceId, CompactDataTables))
      .thenReturn(Future.successful(true))
    when(workspaceSettingRepository.getWorkspaceSettingOfType(workspaceId, CompactDataTables))
      .thenReturn(Future.successful(Option(CompactDataTablesSetting(CompactDataTablesConfig(enabled = true)))))
    val afterSettingPending = intercept[DataEntityException] {
      Await.result(entityManager.resolveProviderFuture(entityRequestArguments), Duration.Inf)
    }
    afterSettingPending.getMessage should include("migration is in progress")
  }

}
