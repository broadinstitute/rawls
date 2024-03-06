package org.broadinstitute.dsde.rawls.monitor

import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO.PubSubMessage
import org.broadinstitute.dsde.rawls.model.{RawlsUserEmail, WorkspaceName}

import java.util.UUID

class AvroUpsertMonitorMessageParser(message: PubSubMessage) {

  def parse: AvroUpsertAttributes = {
    val workspaceNamespace = "workspaceNamespace"
    val workspaceName = "workspaceName"
    val userEmail = "userEmail"
    val jobId = "jobId"
    val upsertFile = "upsertFile"
    val isUpsert = "isUpsert"

    def attributeNotFoundException(attribute: String): Nothing = throw new Exception(
      s"unable to parse message - attribute $attribute not found in ${message.attributes}"
    )

    AvroUpsertAttributes(
      WorkspaceName(
        message.attributes.getOrElse(workspaceNamespace, attributeNotFoundException(workspaceNamespace)),
        message.attributes.getOrElse(workspaceName, attributeNotFoundException(workspaceName))
      ),
      RawlsUserEmail(message.attributes.getOrElse(userEmail, attributeNotFoundException(userEmail))),
      UUID.fromString(message.attributes.getOrElse(jobId, attributeNotFoundException(jobId))),
      message.attributes.getOrElse(upsertFile, attributeNotFoundException(upsertFile)),
      java.lang.Boolean.parseBoolean(message.attributes.getOrElse(isUpsert, "true"))
    )
  }

}
