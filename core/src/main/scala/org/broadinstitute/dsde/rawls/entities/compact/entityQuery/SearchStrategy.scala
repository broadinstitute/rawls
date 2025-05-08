package org.broadinstitute.dsde.rawls.entities.compact.entityQuery

import org.broadinstitute.dsde.rawls.entities.compact.CompactEntityRepository
import org.broadinstitute.dsde.rawls.model.EntityQuery
import slick.jdbc.TransactionIsolation.ReadCommitted

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

/**
  * Paginated query that performs a substring search across all attributes.
  */
class SearchStrategy(override val repository: CompactEntityRepository,
                     workspaceId: UUID,
                     entityType: String,
                     entityQuery: EntityQuery
)(implicit val executionContext: ExecutionContext)
    extends EntityQueryStrategy {
  override def getCountAndSource: Future[CountAndSource] =
    repository.dataSource
      .inTransaction(ReadCommitted) { _ =>
        repository.queries.countEntitiesWithFilterTerms(workspaceId, entityType, entityQuery)
      }
      .map { count =>
        CountAndSource(
          count,
          streamQuery(count, repository.queries.queryEntitiesWithFilterTerms(workspaceId, entityType, entityQuery))
        )
      }
}
