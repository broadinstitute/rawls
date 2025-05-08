package org.broadinstitute.dsde.rawls.entities.compact.entityQuery

import org.broadinstitute.dsde.rawls.entities.compact.CompactEntityRepository
import org.broadinstitute.dsde.rawls.model.EntityQuery
import slick.jdbc.TransactionIsolation.ReadCommitted

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

/**
  * Paginated query with an exact-match filter on a single column.
  */
class FilterByColumnStrategy(override val repository: CompactEntityRepository,
                             workspaceId: UUID,
                             entityType: String,
                             entityQuery: EntityQuery
)(implicit val executionContext: ExecutionContext)
    extends EntityQueryStrategy {
  override def getCountAndSource: Future[CountAndSource] = {
    val columnFilter = entityQuery.columnFilter.get
    repository.dataSource
      .inTransaction(ReadCommitted) { _ =>
        repository.queries.countEntitiesWithColumnFilter(workspaceId, entityType, columnFilter)
      }
      .map { count =>
        CountAndSource(
          count,
          streamQuery(count,
                      repository.queries
                        .queryEntitiesWithColumnFilter(workspaceId, entityType, entityQuery, columnFilter)
          )
        )
      }
  }
}
