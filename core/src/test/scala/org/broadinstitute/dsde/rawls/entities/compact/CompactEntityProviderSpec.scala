package org.broadinstitute.dsde.rawls.entities.compact

import akka.NotUsed
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.scaladsl.Source
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.{CompactEntityRecord, DataAccess, ReadWriteAction}
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.base.{ExpressionEvaluationContext, ExpressionValidator}
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationSupport.LookupExpression
import org.broadinstitute.dsde.rawls.entities.exceptions.EntityNotFoundException
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.model.{
  AttributeEntityReference,
  AttributeEntityReferenceList,
  AttributeName,
  AttributeNumber,
  AttributeRename,
  AttributeString,
  AttributeUpdateOperations,
  AttributeValue,
  Entity,
  EntityQuery,
  EntityQueryResponse,
  EntityQueryResultMetadata,
  EntityTypeMetadata,
  EntityTypeRename,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  SubmissionValidationEntityInputs,
  UserInfo,
  Workspace
}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import slick.dbio.SuccessAction
import slick.jdbc.MySQLProfile.api._

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Try

class CompactEntityProviderSpec extends AnyFlatSpec with Matchers with MockitoTestUtils {

  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  val defaultRequestContext: RawlsRequestContext =
    RawlsRequestContext(
      UserInfo(RawlsUserEmail("test"), OAuth2BearerToken("Bearer 123"), 123, RawlsUserSubjectId("abc"))
    )

  val defaultWorkspace: Workspace = Workspace(
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

  val defaultEntityRequestArguments: EntityRequestArguments =
    EntityRequestArguments(defaultWorkspace, defaultRequestContext)

  // ====================================================================================================
  // tests for CompactEntityProvider public implementations of EntityProvider methods
  // ====================================================================================================

  "batchUpdateEntities" should "have tests" is pending
  "batchUpsertEntities" should "have tests" is pending
  "copyEntities" should "have tests" is pending
  "createEntity" should "have tests" is pending
  "deleteEntities" should "have tests" is pending
  "deleteEntitiesOfType" should "have tests" is pending
  "deleteEntityAttributes" should "have tests" is pending
  "entityTypeMetadata" should "have tests" is pending
  "evaluateExpression" should "have tests" is pending
  "evaluateExpressions" should "have tests" is pending
  "expressionValidator" should "have tests" is pending

  behavior of "getEntity"

  it should "return Entity when row exists in db" in {
    val rec: CompactEntityRecord =
      CompactEntityRecord(1, "name", "type", UUID.randomUUID(), -1, deleted = false, Some("{}"))

    // mocks
    val mockDataAccess: DataAccess = mock[DataAccess]
    val mockCompactEntityQuery = mock[mockDataAccess.CompactEntityQuery]
    when(mockCompactEntityQuery.getEntity(any[UUID], anyString(), anyString()))
      .thenReturn(DBIO.successful(Some(rec)))
    when(mockDataAccess.getCompactEntityQuery).thenReturn(mockCompactEntityQuery)
    // provider using mocks
    val compactEntityProvider = mockingProvider(mockDataAccess)

    val actual = Await.result(compactEntityProvider.getEntity("nonexistent-type", "nonexistent-name"), Duration.Inf)

    val expected = Entity(rec.name, rec.entityType, Map())

    actual shouldBe expected
  }

  it should "return Entity with its attributes" in {
    val attrString = """{"foo":"bar", "baz": 42}"""
    val rec: CompactEntityRecord =
      CompactEntityRecord(1, "name", "type", UUID.randomUUID(), -1, deleted = false, Some(attrString))

    // mocks
    val mockDataAccess: DataAccess = mock[DataAccess]
    val mockCompactEntityQuery = mock[mockDataAccess.CompactEntityQuery]
    when(mockCompactEntityQuery.getEntity(any[UUID], anyString(), anyString()))
      .thenReturn(DBIO.successful(Some(rec)))
    when(mockDataAccess.getCompactEntityQuery).thenReturn(mockCompactEntityQuery)
    // provider using mocks
    val compactEntityProvider = mockingProvider(mockDataAccess)

    val actual = Await.result(compactEntityProvider.getEntity("nonexistent-type", "nonexistent-name"), Duration.Inf)

    val expected = Entity(rec.name,
                          rec.entityType,
                          Map(
                            AttributeName.withDefaultNS("foo") -> AttributeString("bar"),
                            AttributeName.withDefaultNS("baz") -> AttributeNumber(42)
                          )
    )

    actual shouldBe expected
  }

  it should "throw EntityNotFoundException if row not found in db" in {
    // mocks
    val mockDataAccess: DataAccess = mock[DataAccess]
    val mockCompactEntityQuery = mock[mockDataAccess.CompactEntityQuery]
    when(mockCompactEntityQuery.getEntity(any[UUID], anyString(), anyString()))
      .thenReturn(DBIO.successful(None))
    when(mockDataAccess.getCompactEntityQuery).thenReturn(mockCompactEntityQuery)
    // provider using mocks
    val compactEntityProvider = mockingProvider(mockDataAccess)

    val actual = intercept[EntityNotFoundException] {
      Await.result(compactEntityProvider.getEntity("nonexistent-type", "nonexistent-name"), Duration.Inf)
    }
    actual shouldBe a[EntityNotFoundException]
  }

  "listEntities" should "have tests" is pending
  "queryEntities" should "have tests" is pending
  "queryEntitiesSource" should "have tests" is pending
  "renameAttribute" should "have tests" is pending
  "renameEntity" should "have tests" is pending
  "renameEntityType" should "have tests" is pending
  "updateEntity" should "have tests" is pending

  // ====================================================================================================
  // tests for CompactEntityProvider helper methods
  // ====================================================================================================

  behavior of "findAllReferences"

  it should "return nothing if entity has no references" in {
    val attributes = Map(
      AttributeName.withDefaultNS("foo") -> AttributeString("bar"),
      AttributeName.withDefaultNS("baz") -> AttributeNumber(42)
    )
    val entity = Entity("name", "type", attributes)
    val provider = new CompactEntityProvider(defaultEntityRequestArguments, mock[SlickDataSource])

    val actual: Map[AttributeName, Seq[AttributeEntityReference]] = provider.findAllReferences(entity)

    actual shouldBe empty
  }

  it should "find references in the entity's attributes" in {
    val attributes = Map(
      AttributeName.withDefaultNS("foo") -> AttributeString("bar"),
      AttributeName.withDefaultNS("baz") -> AttributeNumber(42),
      AttributeName.withDefaultNS("ref") -> AttributeEntityReference("refTypeA", "refName1"),
      AttributeName.withDefaultNS("reflist") -> AttributeEntityReferenceList(
        Seq(
          AttributeEntityReference("refTypeB", "refName2"),
          AttributeEntityReference("refTypeB", "refName3")
        )
      )
    )
    val entity = Entity("name", "type", attributes)
    val provider = new CompactEntityProvider(defaultEntityRequestArguments, mock[SlickDataSource])

    val actual: Map[AttributeName, Seq[AttributeEntityReference]] = provider.findAllReferences(entity)

    val expected = Map(
      AttributeName.withDefaultNS("ref") -> Seq(AttributeEntityReference("refTypeA", "refName1")),
      AttributeName.withDefaultNS("reflist") ->
        Seq(
          AttributeEntityReference("refTypeB", "refName2"),
          AttributeEntityReference("refTypeB", "refName3")
        )
    )

    actual shouldBe expected
  }

  // ====================================================================================================
  //  helper methods
  // ====================================================================================================

  private def mockingProvider(mockDataAccess: DataAccess): CompactEntityProvider = {
    // mock for DataSource
    val mockDataSource = mock[SlickDataSource]
    when(
      mockDataSource.inTransaction[Option[CompactEntityRecord]](
        any[DataAccess => ReadWriteAction[Option[CompactEntityRecord]]],
        any()
      )
    )
      .thenAnswer { answer =>
        val innerFunc = answer.getArgument[DataAccess => ReadWriteAction[Option[CompactEntityRecord]]](0)
        val dbResult = innerFunc(mockDataAccess)
        dbResult match {
          case SuccessAction(v) => Future.successful(v)
          case x                => fail(s"inTransaction mock received wrong result: $x")
        }
      }

    // create provider
    new CompactEntityProvider(defaultEntityRequestArguments, mockDataSource)
  }

}
