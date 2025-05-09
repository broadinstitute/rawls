package org.broadinstitute.dsde.rawls.entities.compact.entityQuery

import akka.NotUsed
import akka.stream.scaladsl.Source
import org.broadinstitute.dsde.rawls.dataaccess.slick.CompactEntityRecord
import org.broadinstitute.dsde.rawls.entities.compact.CompactEntityRepository
import org.broadinstitute.dsde.rawls.model.{Attributable, AttributeName, Entity, EntityColumnFilter, EntityQuery}
import slick.dbio.Effect
import slick.jdbc.TransactionIsolation.ReadCommitted
import slick.jdbc.{ResultSetConcurrency, ResultSetType}
import slick.sql.SqlStreamingAction

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

case class CountAndSource(count: Int, source: Source[Entity, _])

trait EntityQueryStrategy {
  val repository: CompactEntityRepository

  def getCountAndSource: Future[CountAndSource]

  protected def streamQuery(
    count: Int,
    query: SqlStreamingAction[Seq[Entity], Entity, Effect.Read]
  ): Source[Entity, NotUsed] = {
    import repository.dataSource.dataAccess.driver.api._
    if (count == 0) {
      // if there are no results, we can just return an empty source
      Source.empty
    } else {
      // otherwise, we need to stream the results
      Source.fromPublisher(
        repository.dataSource.database.stream(
          query.transactionally
            .withTransactionIsolation(ReadCommitted)
            .withStatementParameters(rsType = ResultSetType.ForwardOnly,
                                     rsConcurrency = ResultSetConcurrency.ReadOnly,
                                     fetchSize = repository.dataSource.fetchSize
            )
        )
      )
    }
  }
}

object EntityQueryStrategy {

  /**
   * Determines the query strategy based on the following cases:
   *
   * 1. `filterTerms` is defined.
   * 2. `columnFilter` is defined and `attributeName` is `idAttributeName` (should have 0 or 1 results).
   * 3. `columnFilter` is defined and `attributeName` is not `idAttributeName`.
   * 4. Neither `filterTerms` nor `columnFilter` is defined.
   */
  def choose(repository: CompactEntityRepository,
             workspaceId: UUID,
             entityType: String,
             entityQuery: EntityQuery,
             unfilteredCount: Int
  )(implicit executionContext: ExecutionContext): EntityQueryStrategy = {
    val idAttributeName = AttributeName.withDefaultNS(entityType + Attributable.entityIdAttributeSuffix)

    (entityQuery.filterTerms, entityQuery.columnFilter) match {
      case (Some(_), _) =>
        new SearchStrategy(repository, workspaceId, entityType, entityQuery)
      case (_, Some(EntityColumnFilter(`idAttributeName`, _))) =>
        new FilterByNameStrategy(repository, workspaceId, entityType, entityQuery)
      case (_, Some(_)) =>
        new FilterByColumnStrategy(repository, workspaceId, entityType, entityQuery)
      case _ =>
        new AllEntitiesStrategy(repository, workspaceId, entityType, entityQuery, unfilteredCount)
    }
  }
}
