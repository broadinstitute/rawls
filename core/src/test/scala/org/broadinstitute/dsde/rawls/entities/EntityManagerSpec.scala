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
    val mockDataSource = mock[SlickDataSource]

    val entityManager: EntityManager = EntityManager.defaultEntityManager(
      mockDataSource,
      metricsPrefix = "metrics-prefix"
    )

    val entityRequestArguments = EntityRequestArguments(workspace, defaultRequestContext)

    val provider = Await.result(entityManager.resolveProviderFuture(entityRequestArguments), Duration.Inf)

    // Provider should always be an AuditLoggingEntityProvider with a CompactEntityProvider delegate
    provider shouldBe a[AuditLoggingEntityProvider]
    val afterSettingAuditProvider = provider.asInstanceOf[AuditLoggingEntityProvider]
    afterSettingAuditProvider.delegate shouldBe a[CompactEntityProvider]

  }

}
