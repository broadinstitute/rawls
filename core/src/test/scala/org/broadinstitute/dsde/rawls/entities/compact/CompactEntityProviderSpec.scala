package org.broadinstitute.dsde.rawls.entities.compact

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.{CompactEntityRecord, DataAccess, ReadWriteAction}
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.exceptions.EntityNotFoundException
import org.broadinstitute.dsde.rawls.model.{
  AttributeName,
  AttributeNumber,
  AttributeString,
  Entity,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
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
