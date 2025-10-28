package org.broadinstitute.dsde.rawls.entities.local

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction, TestDriverComponent}
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigTestSupport
import org.broadinstitute.dsde.rawls.metrics.StatsDTestUtils
import org.broadinstitute.dsde.rawls.model.AttributeName.toDelimitedName
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.EntityUpdateDefinition
import org.broadinstitute.dsde.rawls.model.{AttributeName, AttributeString, Entity, EntityTypeMetadata}
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AddUpdateAttribute, EntityUpdateDefinition}
import org.broadinstitute.dsde.rawls.model.{
  AttributeName,
  AttributeNumber,
  AttributeString,
  AttributeValueEmptyList,
  AttributeValueList,
  Entity,
  EntityQuery,
  MethodConfiguration,
  RawlsRequestContext,
  SortDirections,
  SubmissionValidationValue,
  WDL,
  Workspace
}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.scalatest.RecoverMethods.recoverToExceptionIf
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Span}
import org.scalatest.wordspec.AnyWordSpecLike
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.GatherInputsResult

import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.concurrent.ExecutionContext

class LocalEntityProviderSpec
    extends AnyWordSpecLike
    with Matchers
    with ScalaFutures
    with TestDriverComponent
    with MethodConfigTestSupport
    with StatsDTestUtils
    with Eventually
    with MockitoTestUtils {
  import driver.api._

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(3000, Millis)))

  implicit private val system: ActorSystem = ActorSystem("LocalEntityProviderSpec")

  val testConf = ConfigFactory.load()

  "LocalEntityProvider" should {

    "accept multiple update operations for the same entity in batchUpsert" in withLocalEntityProviderTestDatabase {
      dataSource =>
        val workspaceContext = runAndWait(
          dataSource.dataAccess.workspaceQuery.findById(localEntityProviderTestData.workspace.workspaceId)
        ).get
        val localEntityProvider = new LocalEntityProvider(
          EntityRequestArguments(workspaceContext, testContext),
          slickDataSource,
          cacheEnabled = true,
          testConf.getDuration("entities.queryTimeout"),
          workbenchMetricBaseName
        )

        val multiUpsert = Seq(
          EntityUpdateDefinition("myname",
                                 "mytype",
                                 Seq(AddUpdateAttribute(AttributeName.withDefaultNS("one"), AttributeString("111")))
          ),
          EntityUpdateDefinition("myname",
                                 "mytype",
                                 Seq(AddUpdateAttribute(AttributeName.withDefaultNS("two"), AttributeString("222")))
          )
        )
        val writes = localEntityProvider.batchUpsertEntities(Source(multiUpsert), testContext).futureValue

        writes shouldBe 2

        val entityQuery = EntityQuery(1, 100, "name", SortDirections.Ascending, None)
        val parentContext = RawlsRequestContext(userInfo)
        val actual = localEntityProvider.queryEntities("mytype", entityQuery, parentContext).futureValue

        actual.resultMetadata.unfilteredCount shouldBe 1
        actual.results.size shouldBe 1
        actual.results.head shouldBe Entity("myname",
                                            "mytype",
                                            Map(AttributeName.withDefaultNS("one") -> AttributeString("111"),
                                                AttributeName.withDefaultNS("two") -> AttributeString("222")
                                            )
        )

        val withAllAttrs = runAndWait(
          dataSource.dataAccess.entityQueryWithInlineAttributes
            .findEntityByName(localEntityProviderTestData.workspace.workspaceIdAsUUID, "mytype", "myname")
            .result
        )

        withAllAttrs.size shouldBe 1
        val allAttrs = withAllAttrs.head.allAttributeValues.getOrElse("")
        allAttrs shouldBe "myname 111 222"
    }

  }

  "LocalEntityProvider case-sensitivity" should {
    "return helpful error message when upserting case-divergent entity names (createEntity method)" in withLocalEntityProviderTestDatabase {
      dataSource =>
        val workspaceContext = runAndWait(
          dataSource.dataAccess.workspaceQuery.findById(localEntityProviderTestData.workspace.workspaceId)
        ).get
        val localEntityProvider = new LocalEntityProvider(
          EntityRequestArguments(workspaceContext, testContext),
          slickDataSource,
          cacheEnabled = true,
          testConf.getDuration("entities.queryTimeout"),
          workbenchMetricBaseName
        )

        // create the first entity with name "myname"
        val entity1 = Entity("myname", "casetest", Map())
        val created1 = localEntityProvider.createEntity(entity1, testContext).futureValue
        created1 shouldBe entity1

        // attempt to create the second entity with name "MyName" - differing from entity1's name only in case
        val entity2 = Entity("MyName", "casetest", Map())
        val ex = recoverToExceptionIf[Exception] {
          localEntityProvider.createEntity(entity2, testContext)
        }.futureValue

        ex match {
          case er: RawlsExceptionWithErrorReport =>
            val expectedMessage =
              s"${entity2.entityType} ${entity2.name} already exists in ${workspaceContext.toWorkspaceName}"
            er.errorReport.message shouldBe expectedMessage
          case _ =>
            fail(
              s"expected a RawlsExceptionWithErrorReport, found ${ex.getClass.getName} with message '${ex.getMessage}''"
            )
        }
    }

    "return helpful error message when upserting case-divergent entity names (batchUpsertEntities method)" in withLocalEntityProviderTestDatabase {
      dataSource =>
        val workspaceContext = runAndWait(
          dataSource.dataAccess.workspaceQuery.findById(localEntityProviderTestData.workspace.workspaceId)
        ).get
        val localEntityProvider = new LocalEntityProvider(
          EntityRequestArguments(workspaceContext, testContext),
          slickDataSource,
          cacheEnabled = true,
          testConf.getDuration("entities.queryTimeout"),
          workbenchMetricBaseName
        )

        // create the first entity with name "myname"
        val upsert1 = Seq(EntityUpdateDefinition("myname", "casetest", Seq()))
        val created1 = localEntityProvider.batchUpsertEntities(Source(upsert1), testContext).futureValue
        created1 shouldBe 1

        // attempt to create the second entity with name "MyName" - differing from entity1's name only in case
        val upsert2 = Seq(EntityUpdateDefinition("MyName", "casetest", Seq()))
        val ex = recoverToExceptionIf[Exception] {
          localEntityProvider.batchUpsertEntities(Source(upsert2), testContext)
        }.futureValue

        ex match {
          case er: RawlsExceptionWithErrorReport =>
            val expectedMessage =
              "Database error occurred. Check if you are uploading entity names that differ only in case from pre-existing entities."
            er.errorReport.message shouldBe expectedMessage
          case _ =>
            fail(
              s"expected a RawlsExceptionWithErrorReport, found ${ex.getClass.getName} with message '${ex.getMessage}''"
            )
        }
    }
  }

}
