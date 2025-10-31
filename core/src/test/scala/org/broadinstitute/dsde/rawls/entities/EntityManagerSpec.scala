package org.broadinstitute.dsde.rawls.entities

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.entities.base.AuditLoggingEntityProvider
import org.broadinstitute.dsde.rawls.entities.compact.CompactEntityProvider
import org.broadinstitute.dsde.rawls.model.{
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  UserInfo,
  Workspace
}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.joda.time.DateTime
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.Await

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

  "EntityManager" should "always return CompactEntityProvider" in {
    val mockDataSource = mock[SlickDataSource]
    val entityManager: EntityManager = EntityManager.defaultEntityManager(
      mockDataSource,
      metricsPrefix = "metrics-prefix"
    )
    val entityRequestArguments = EntityRequestArguments(workspace, defaultRequestContext)
    // Provider should always be an AuditLoggingEntityProvider with a CompactEntityProvider delegate
    val provider = Await.result(entityManager.resolveProviderFuture(entityRequestArguments), Duration.Inf)
    provider shouldBe a[AuditLoggingEntityProvider]
    val auditProvider = provider.asInstanceOf[AuditLoggingEntityProvider]
    auditProvider.delegate shouldBe a[CompactEntityProvider]
  }

}
