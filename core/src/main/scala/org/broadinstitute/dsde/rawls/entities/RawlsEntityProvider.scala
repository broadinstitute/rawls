package org.broadinstitute.dsde.rawls.entities

import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.{SlickDataSource, SlickWorkspaceContext}
import org.broadinstitute.dsde.rawls.model.{Entity, ErrorReport, Workspace}

import scala.concurrent.{ExecutionContext, Future}

/**
 * Terra default entity provider, powered by Rawls and Cloud SQL
 */
class RawlsEntityProvider(workspace: Workspace, dataSource: SlickDataSource)
                         (implicit protected val executionContext: ExecutionContext)
  extends EntityProvider with LazyLogging {

  import dataSource.dataAccess.driver.api._

  override def createEntity(entity: Entity): Future[Entity] = {
    val workspaceContext = SlickWorkspaceContext(workspace)

    dataSource.inTransaction { dataAccess =>
      dataAccess.entityQuery.get(workspaceContext, entity.entityType, entity.name) flatMap {
        case Some(_) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"${entity.entityType} ${entity.name} already exists in ${workspace.toWorkspaceName}")))
        case None => dataAccess.entityQuery.save(workspaceContext, entity)
      }
    }
  }

}
