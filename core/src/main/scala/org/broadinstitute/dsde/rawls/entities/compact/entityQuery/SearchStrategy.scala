package org.broadinstitute.dsde.rawls.entities.compact.entityQuery

import akka.http.scaladsl.model.StatusCodes
import akka.stream.scaladsl.Source
import io.sentry.Sentry
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.entities.EntityUtils
import org.broadinstitute.dsde.rawls.entities.compact.CompactEntityRepository
import org.broadinstitute.dsde.rawls.model.{Entity, EntityQuery, ErrorReport}
import org.broadinstitute.dsde.rawls.webservice.RawlsApiService.logger
import slick.jdbc.TransactionIsolation
import slick.jdbc.TransactionIsolation.ReadCommitted

import java.sql.SQLException
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

/**
  * Paginated query that performs a substring search across all attributes.
  */
class SearchStrategy(override val repository: CompactEntityRepository,
                     workspaceId: UUID,
                     entityType: String,
                     entityQuery: EntityQuery,
                     filterTerms: Seq[String],
                     override val workbenchMetricBaseName: String
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
      .flatMap { count =>
        withSortMemoryRetries[Seq[Entity]](entityQuery,
                                           this.getClass.getSimpleName,
                                           isolationLevel = TransactionIsolation.ReadCommitted
        ) {
          repository.queries.queryEntitiesWithFilterTerms(workspaceId, entityType, entityQuery, filterTerms)
        } map { sourceQueryMaterializedResult =>
          val source = Source(sourceQueryMaterializedResult)
          CountAndSource(
            count,
            source
          )
        }
      }
  }
}
