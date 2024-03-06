package org.broadinstitute.dsde.rawls.monitor

import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO.PubSubMessage
import org.broadinstitute.dsde.rawls.model.{RawlsUserEmail, WorkspaceName}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class AvroUpsertMonitorMessageParser(message: PubSubMessage, dataSource: SlickDataSource)(implicit
  executionContext: ExecutionContext
) {

  def parse: Future[AvroUpsertAttributes] = {
    val workspaceNamespace = "workspaceNamespace"
    val workspaceName = "workspaceName"
    val userEmail = "userEmail"
    val jobId = "jobId"
    val upsertFile = "upsertFile"
    val isUpsert = "isUpsert"

    val workspaceId = "workspaceId"

    def attributeNotFoundException(attribute: String): Nothing = throw new Exception(
      s"unable to parse message - attribute $attribute not found in ${message.attributes}"
    )

    // does this message contain a workspaceId? This indicates a message from cWDS instead of a message
    // from Import Service.
    val isWdsMessage = message.attributes.contains(workspaceId)

    val workspaceNameFuture: Future[WorkspaceName] = if (isWdsMessage) {
      // translate the workspaceId into a namespace/name
      val id = message.attributes.getOrElse(workspaceId, attributeNotFoundException(workspaceId))
      dataSource.inTransaction { dataAccess =>
        dataAccess.workspaceQuery.findByIdOrFail(id) map { workspace =>
          WorkspaceName(workspace.namespace, workspace.name)
        }
      }
    } else {
      Future.successful(
        WorkspaceName(
          message.attributes.getOrElse(workspaceNamespace, attributeNotFoundException(workspaceNamespace)),
          message.attributes.getOrElse(workspaceName, attributeNotFoundException(workspaceName))
        )
      )
    }

    workspaceNameFuture map { workspaceName =>
      AvroUpsertAttributes(
        workspaceName,
        RawlsUserEmail(message.attributes.getOrElse(userEmail, attributeNotFoundException(userEmail))),
        UUID.fromString(message.attributes.getOrElse(jobId, attributeNotFoundException(jobId))),
        message.attributes.getOrElse(upsertFile, attributeNotFoundException(upsertFile)),
        java.lang.Boolean.parseBoolean(message.attributes.getOrElse(isUpsert, "true"))
      )
    }
  }

}
