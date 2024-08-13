package org.broadinstitute.dsde.rawls.monitor

import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO.PubSubMessage
import org.broadinstitute.dsde.rawls.model.RawlsUserEmail
import org.broadinstitute.dsde.rawls.monitor.AvroUpsertMonitorMessageParser._

import java.util.UUID

object AvroUpsertMonitorMessageParser {
  val userEmail = "userEmail"
  val jobId = "jobId"
  val upsertFile = "upsertFile"
  val isUpsert = "isUpsert"
  val workspaceId = "workspaceId"
}

class AvroUpsertMonitorMessageParser(message: PubSubMessage) {

  def parse: CwdsUpsertAttributes = {

    def attributeNotFoundException(attribute: String): Nothing = throw new Exception(
      s"unable to parse message - attribute $attribute not found in ${message.attributes}"
    )

    CwdsUpsertAttributes(
      UUID.fromString(message.attributes.getOrElse(workspaceId, attributeNotFoundException(workspaceId))),
      RawlsUserEmail(message.attributes.getOrElse(userEmail, attributeNotFoundException(userEmail))),
      UUID.fromString(message.attributes.getOrElse(jobId, attributeNotFoundException(jobId))),
      message.attributes.getOrElse(upsertFile, attributeNotFoundException(upsertFile)),
      java.lang.Boolean.parseBoolean(message.attributes.getOrElse(isUpsert, "true"))
    )
  }

}
