package org.broadinstitute.dsde.rawls.entities.local

import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.{SlickDataSource, SlickWorkspaceContext}
import org.broadinstitute.dsde.rawls.entities.base.EntityProvider
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, Entity, EntityTypeMetadata, ErrorReport, Workspace}

import scala.concurrent.{ExecutionContext, Future}

/**
 * Terra default entity provider, powered by Rawls and Cloud SQL
 */
class LocalEntityProvider(workspace: Workspace, dataSource: SlickDataSource)
                         (implicit protected val executionContext: ExecutionContext)
  extends EntityProvider with LazyLogging {

  import dataSource.dataAccess.driver.api._

  private val workspaceContext = SlickWorkspaceContext(workspace)

  override def entityTypeMetadata(): Future[Map[String, EntityTypeMetadata]] = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.entityQuery.getEntityTypeMetadata(workspaceContext)
    }
  }

  override def createEntity(entity: Entity): Future[Entity] = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.entityQuery.get(workspaceContext, entity.entityType, entity.name) flatMap {
        case Some(_) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"${entity.entityType} ${entity.name} already exists in ${workspace.toWorkspaceName}")))
        case None => dataAccess.entityQuery.save(workspaceContext, entity)
      }
    }
  }

  override def deleteEntities(entRefs: Seq[AttributeEntityReference]): Future[Int] = ???
}
