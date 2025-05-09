package org.broadinstitute.dsde.rawls.entities.compact.entityQuery

import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.scaladsl.{Sink, Source}
import org.broadinstitute.dsde.rawls.dataaccess.slick.{CompactEntityRecord, TestDriverComponentWithFlatSpecAndMatchers}
import org.broadinstitute.dsde.rawls.entities.compact.CompactEntityRepository
import org.broadinstitute.dsde.rawls.model.{
  Attributable,
  AttributeName,
  EntityColumnFilter,
  EntityQuery,
  SortDirections
}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.mockito.Mockito.when
import slick.jdbc.GetResult

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class EntityQueryStrategySpec
    extends TestDriverComponentWithFlatSpecAndMatchers
    with MockitoTestUtils
    with ScalatestRouteTest {
  behavior of "EntityQueryStrategy.choose"

  it should "choose the SearchStrategy when filterTerms is defined" in {
    val repository = mock[CompactEntityRepository]
    val entityQuery = EntityQuery(0, 0, "", SortDirections.Ascending, Some("test"))
    val strategy = EntityQueryStrategy.choose(repository, UUID.randomUUID(), "testType", entityQuery, 0)

    strategy shouldBe a[SearchStrategy]
  }

  it should "choose the FilterByNameStrategy when columnFilter is defined and attributeName is idAttributeName" in {
    val repository = mock[CompactEntityRepository]
    val entityType = "testType"
    val entityQuery = EntityQuery(
      0,
      0,
      "",
      SortDirections.Ascending,
      None,
      columnFilter =
        Some(EntityColumnFilter(AttributeName.withDefaultNS(entityType + Attributable.entityIdAttributeSuffix), "test"))
    )
    val strategy = EntityQueryStrategy.choose(repository, UUID.randomUUID(), entityType, entityQuery, 1)

    strategy shouldBe a[FilterByNameStrategy]
  }

  it should "choose the FilterByColumnStrategy when columnFilter is defined and attributeName is not idAttributeName" in {
    val repository = mock[CompactEntityRepository]
    val entityQuery = EntityQuery(0,
                                  0,
                                  "",
                                  SortDirections.Ascending,
                                  None,
                                  columnFilter = Some(EntityColumnFilter(AttributeName.withDefaultNS("foo"), "test"))
    )
    val strategy = EntityQueryStrategy.choose(repository, UUID.randomUUID(), "testType", entityQuery, 1)

    strategy shouldBe a[FilterByColumnStrategy]
  }

  it should "choose the SearchStrategy when neither filterTerms nor columnFilter is defined" in {
    val repository = mock[CompactEntityRepository]
    val entityQuery = EntityQuery(0, 0, "", SortDirections.Ascending, None)
    val strategy = EntityQueryStrategy.choose(repository, UUID.randomUUID(), "testType", entityQuery, 0)

    strategy shouldBe a[AllEntitiesStrategy]
  }

  behavior of "streamQuery"

  it should "return an empty source when count is 0" in {
    val strategy = new EntityQueryStrategy {
      override val repository: CompactEntityRepository = mock[CompactEntityRepository]
      override def getCountAndSource: Future[CountAndSource] =
        // the null will cause a NPE if streamQuery tries to use it which it shouldn't
        Future.successful(CountAndSource(0, streamQuery(0, null)))
    }

    Await.result(strategy.getCountAndSource, Duration.Inf).source shouldBe Source.empty
  }

  it should "return a non-empty source when count is greater than 0" in {
    import slick.jdbc.MySQLProfile.api._
    val testValue = CompactEntityRecord(0, "name", "type", UUID.randomUUID(), 1, false, None)
    implicit val testCompactEntityGetter: GetResult[CompactEntityRecord] = GetResult(_ => testValue)

    val strategy = new EntityQueryStrategy {
      override val repository: CompactEntityRepository = mock[CompactEntityRepository]
      override def getCountAndSource: Future[CountAndSource] = {
        when(repository.dataSource).thenReturn(slickDataSource)
        Future.successful(CountAndSource(1, streamQuery(1, sql"SELECT 1".as[CompactEntityRecord])))
      }
    }

    val testFuture = for {
      countAndSource <- strategy.getCountAndSource
      results <- countAndSource.source.runWith(Sink.seq)
    } yield results
    Await.result(testFuture, Duration.Inf) should contain theSameElementsAs Seq(testValue)
  }
}
