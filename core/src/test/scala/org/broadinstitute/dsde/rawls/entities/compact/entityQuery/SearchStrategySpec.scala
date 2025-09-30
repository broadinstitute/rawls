package org.broadinstitute.dsde.rawls.entities.compact.entityQuery

import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.scaladsl.Sink
import org.broadinstitute.dsde.rawls.dataaccess.slick.{CompactEntityRecord, TestDriverComponentWithFlatSpecAndMatchers}
import org.broadinstitute.dsde.rawls.entities.compact.CompactEntityRepository
import org.broadinstitute.dsde.rawls.model.{Entity, EntityQuery, SortDirections}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.mockito.Mockito.when
import slick.jdbc.GetResult

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class SearchStrategySpec
    extends TestDriverComponentWithFlatSpecAndMatchers
    with MockitoTestUtils
    with ScalatestRouteTest {
  behavior of "getCountAndSource"

  it should "return the count and results" in {
    import slick.jdbc.MySQLProfile.api._
    val mockRepository = mock[CompactEntityRepository]
    val mockQuery = mock[slickDataSource.dataAccess.compactEntityQuery.type]
    when(mockRepository.dataSource).thenReturn(slickDataSource)
    when(mockRepository.queries).thenReturn(mockQuery)

    val testValue = Entity("name", "type", Map.empty)
    implicit val testCompactEntityGetter: GetResult[Entity] = GetResult(_ => testValue)

    val entityQuery = EntityQuery(0, 0, "", SortDirections.Ascending, Some("test"))
    val workspaceId = UUID.randomUUID()
    val entityType = "testType"
    val strategy =
      new SearchStrategy(mockRepository, workspaceId, entityType, entityQuery, Seq("test"), "testMetricPrefix")

    when(mockRepository.queries.countEntitiesWithFilterTerms(workspaceId, entityType, entityQuery, Seq("test")))
      .thenReturn(DBIO.successful(10))
    when(mockRepository.queries.queryEntitiesWithFilterTerms(workspaceId, entityType, entityQuery, Seq("test")))
      .thenReturn(sql"SELECT 1".as[Entity])

    val testFuture = for {
      countAndSource <- strategy.getCountAndSource
      results <- countAndSource.source.runWith(Sink.seq)
    } yield (countAndSource.count, results)
    Await.result(testFuture, Duration.Inf)._1 shouldBe 10
    Await.result(testFuture, Duration.Inf)._2 should contain theSameElementsAs Seq(testValue)
  }
}
