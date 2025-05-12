package org.broadinstitute.dsde.rawls.entities.compact.entityQuery

import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.scaladsl.Sink
import org.broadinstitute.dsde.rawls.dataaccess.slick.{CompactEntityRecord, TestDriverComponentWithFlatSpecAndMatchers}
import org.broadinstitute.dsde.rawls.entities.compact.CompactEntityRepository
import org.broadinstitute.dsde.rawls.model.{AttributeName, EntityColumnFilter, EntityQuery, SortDirections}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.mockito.Mockito.when

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class FilterByNameStrategySpec
    extends TestDriverComponentWithFlatSpecAndMatchers
    with MockitoTestUtils
    with ScalatestRouteTest {
  behavior of "getCountAndSource"

  it should "return the 1 and the entity when there is a match" in {
    import slick.jdbc.MySQLProfile.api._
    val mockRepository = mock[CompactEntityRepository]
    val mockQuery = mock[slickDataSource.dataAccess.compactEntityQuery.type]
    when(mockRepository.dataSource).thenReturn(slickDataSource)
    when(mockRepository.queries).thenReturn(mockQuery)

    val testValue = CompactEntityRecord(0, "name", "type", UUID.randomUUID(), 1, false, None)

    val entityName = "test"
    val columnFilter = EntityColumnFilter(AttributeName.withDefaultNS("name"), entityName)
    val entityQuery = EntityQuery(0, 0, "", SortDirections.Ascending, None, columnFilter = Some(columnFilter))
    val workspaceId = UUID.randomUUID()
    val entityType = "testType"
    val strategy = new FilterByNameStrategy(mockRepository, workspaceId, entityType, entityQuery)

    when(mockRepository.queries.getEntity(workspaceId, entityType, entityName))
      .thenReturn(DBIO.successful(Some(testValue)))

    val testFuture = for {
      countAndSource <- strategy.getCountAndSource
      results <- countAndSource.source.runWith(Sink.seq)
    } yield (countAndSource.count, results)
    Await.result(testFuture, Duration.Inf)._1 shouldBe 1
    Await.result(testFuture, Duration.Inf)._2 should contain theSameElementsAs Seq(testValue.toEntity)
  }

  it should "return 0 and empty when there is no match" in {
    import slick.jdbc.MySQLProfile.api._
    val mockRepository = mock[CompactEntityRepository]
    val mockQuery = mock[slickDataSource.dataAccess.compactEntityQuery.type]
    when(mockRepository.dataSource).thenReturn(slickDataSource)
    when(mockRepository.queries).thenReturn(mockQuery)

    val entityName = "test"
    val columnFilter = EntityColumnFilter(AttributeName.withDefaultNS("name"), entityName)
    val entityQuery = EntityQuery(0, 0, "", SortDirections.Ascending, None, columnFilter = Some(columnFilter))
    val workspaceId = UUID.randomUUID()
    val entityType = "testType"
    val strategy = new FilterByNameStrategy(mockRepository, workspaceId, entityType, entityQuery)

    when(mockRepository.queries.getEntity(workspaceId, entityType, entityName))
      .thenReturn(DBIO.successful(None))

    val testFuture = for {
      countAndSource <- strategy.getCountAndSource
      results <- countAndSource.source.runWith(Sink.seq)
    } yield (countAndSource.count, results)
    Await.result(testFuture, Duration.Inf)._1 shouldBe 0
    Await.result(testFuture, Duration.Inf)._2 shouldBe empty
  }
}
