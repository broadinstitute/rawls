package org.broadinstitute.dsde.rawls.entities.compact.entityQuery

import org.broadinstitute.dsde.rawls.entities.compact.CompactEntityRepository
import org.broadinstitute.dsde.rawls.model.EntityQuery

import java.util.UUID
import scala.concurrent.Future

/**
  * Paginated query without any filters.
  */
class AllEntitiesStrategy(override val repository: CompactEntityRepository,
                          workspaceId: UUID,
                          entityType: String,
                          entityQuery: EntityQuery,
                          unfilteredCount: Int
) extends EntityQueryStrategy {

  override def getCountAndSource: Future[CountAndSource] =
    // there is no filter so we can just use the unfiltered count
    Future.successful(
      CountAndSource(
        unfilteredCount,
        streamQuery(unfilteredCount, repository.queries.queryEntitiesWithNoFilter(workspaceId, entityType, entityQuery))
      )
    )

}
