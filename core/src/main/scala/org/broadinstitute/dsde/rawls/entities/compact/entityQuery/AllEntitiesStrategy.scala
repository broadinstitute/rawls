package org.broadinstitute.dsde.rawls.entities.compact.entityQuery

import akka.stream.scaladsl.Source
import org.broadinstitute.dsde.rawls.entities.compact.CompactEntityRepository
import org.broadinstitute.dsde.rawls.model.{Entity, EntityQuery}
import slick.jdbc.TransactionIsolation

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

/**
  * Paginated query without any filters.
  */
class AllEntitiesStrategy(override val repository: CompactEntityRepository,
                          workspaceId: UUID,
                          entityType: String,
                          entityQuery: EntityQuery,
                          unfilteredCount: Int,
                          override val workbenchMetricBaseName: String
)(implicit
  executionContext: ExecutionContext
) extends EntityQueryStrategy {

  override def getCountAndSource: Future[CountAndSource] =
    withSortMemoryRetries[Seq[Entity]](entityQuery,
                                       this.getClass.getSimpleName,
                                       isolationLevel = TransactionIsolation.ReadCommitted
    ) {
      repository.queries.queryEntitiesWithNoFilter(workspaceId, entityType, entityQuery)
    } map { sourceQueryMaterializedResult =>
      val source = Source(sourceQueryMaterializedResult)
      // there is no filter so we can just use the unfiltered count
      CountAndSource(
        unfilteredCount,
        source
      )
    }
}
