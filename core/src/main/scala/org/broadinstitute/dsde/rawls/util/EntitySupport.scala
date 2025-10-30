package org.broadinstitute.dsde.rawls.util

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction}
import org.broadinstitute.dsde.rawls.model.{
  AttributeEntityReference,
  Entity,
  ErrorReport,
  RawlsRequestContext,
  Workspace
}
import org.broadinstitute.dsde.rawls.util.TracingUtils.traceDBIOWithParent

import scala.concurrent.ExecutionContext

trait EntitySupport {
  implicit protected val executionContext: ExecutionContext
  protected val dataSource: SlickDataSource

  import dataSource.dataAccess.driver.api._

  def withAllEntityRefs[T](workspaceContext: Workspace,
                           dataAccess: DataAccess,
                           entities: Seq[AttributeEntityReference],
                           context: RawlsRequestContext
  )(op: Seq[AttributeEntityReference] => ReadWriteAction[T]): ReadWriteAction[T] =
    // query the db to see which of the specified entity refs exist in the workspace and are active
    traceDBIOWithParent("withAllEntityRefs.getActiveRefs", context)(_ =>
      dataAccess.entityQuery.getActiveRefs(workspaceContext.workspaceIdAsUUID, entities.toSet)
    ) flatMap { found =>
      // were any of the user's entities not found in our query?
      val notFound = entities diff found
      if (notFound.nonEmpty) {
        // generate error messages
        val failureMessages = notFound.map { e =>
          s"${e.entityType} ${e.entityName} does not exist in ${workspaceContext.toWorkspaceName}"
        }
        val err = ErrorReport(statusCode = StatusCodes.BadRequest,
                              message =
                                (Seq("Entities were not found:") ++ failureMessages) mkString System.lineSeparator()
        )
        DBIO.failed(new RawlsExceptionWithErrorReport(err))
      } else {
        op(found)
      }
    }

  def withEntity[T](workspaceContext: Workspace, entityType: String, entityName: String, dataAccess: DataAccess)(
    op: Entity => ReadWriteAction[T]
  ): ReadWriteAction[T] =
    dataAccess.entityQuery.get(workspaceContext, entityType, entityName) flatMap {
      case None =>
        DBIO.failed(
          new RawlsExceptionWithErrorReport(
            errorReport = ErrorReport(StatusCodes.NotFound,
                                      s"$entityType $entityName does not exist in ${workspaceContext.toWorkspaceName}"
            )
          )
        )
      case Some(entity) => op(entity)
    }

}
