package org.broadinstitute.dsde.rawls.monitor

import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO.PubSubMessage
import org.broadinstitute.dsde.rawls.model.{RawlsUserEmail, WorkspaceName}
import org.broadinstitute.dsde.rawls.monitor.AvroUpsertMonitorMessageParser._

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

object AvroUpsertMonitorMessageParser {
  val workspaceNamespace = "workspaceNamespace"
  val workspaceName = "workspaceName"
  val userEmail = "userEmail"
  val jobId = "jobId"
  val upsertFile = "upsertFile"
  val isUpsert = "isUpsert"
  val isCWDS = "isCWDS"
  val workspaceId = "workspaceId"
}

class AvroUpsertMonitorMessageParser(message: PubSubMessage, dataSource: SlickDataSource)(implicit
  executionContext: ExecutionContext
) {

  def parse: Future[AvroUpsertAttributes] = {

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
      } recover { _ =>
        // if we couldn't find the workspaceId above, pass known-bad values.
        // later validation within AvroUpsertMonitor will catch these and handle the missing workspace.
        // the values used here don't pass our validation constraints so they will always be bad.
        // this is a bit of a hack.
        WorkspaceName("**!!error!!**", s"workspaceId:$id")
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
        java.lang.Boolean.parseBoolean(message.attributes.getOrElse(isUpsert, "true")),
        // the "isCWDS" key will be missing from Import Service messages, so it should default to false
        java.lang.Boolean.parseBoolean(message.attributes.getOrElse(isCWDS, "false"))
      )
    }
  }

}
