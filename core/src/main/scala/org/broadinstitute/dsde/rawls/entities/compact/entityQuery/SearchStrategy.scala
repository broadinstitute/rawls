package org.broadinstitute.dsde.rawls.entities.compact.entityQuery

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.entities.compact.CompactEntityRepository
import org.broadinstitute.dsde.rawls.model.{EntityQuery, ErrorReport}
import slick.jdbc.TransactionIsolation.ReadCommitted

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

/**
  * Paginated query that performs a substring search across all attributes.
  */
class SearchStrategy(override val repository: CompactEntityRepository,
                     workspaceId: UUID,
                     entityType: String,
                     entityQuery: EntityQuery,
                     filterTerms: Seq[String]
)(implicit val executionContext: ExecutionContext)
    extends EntityQueryStrategy {
  override def getCountAndSource: Future[CountAndSource] = {
    if (filterTerms.isEmpty) {
      throw new RawlsExceptionWithErrorReport(
        ErrorReport(StatusCodes.BadRequest, "filterTerms must be defined for SearchStrategy")
      )
    }
    repository.dataSource
      .inTransaction(ReadCommitted) { _ =>
        repository.queries.countEntitiesWithFilterTerms(workspaceId, entityType, entityQuery, filterTerms)
      }
      .map { count =>
        CountAndSource(
          count,
          streamQuery(count,
                      repository.queries.queryEntitiesWithFilterTerms(workspaceId, entityType, entityQuery, filterTerms)
          )
        )
      }
  }
}
