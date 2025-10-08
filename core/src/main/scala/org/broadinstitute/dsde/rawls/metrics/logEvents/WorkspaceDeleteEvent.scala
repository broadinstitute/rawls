package org.broadinstitute.dsde.rawls.metrics.logEvents

import java.util

case class WorkspaceDeleteEvent(workspaceId: String,
                                workspaceNamespace: String,
                                workspaceName: String,
                                userSubjectId: String
) extends BardEvent {

  override def eventName: String = "workspace:delete"

  override def getProperties: util.Map[String, Any] =
    this.transformMap(
      Map(
        "workspaceId" -> workspaceId,
        "workspaceNamespace" -> workspaceNamespace,
        "workspaceName" -> workspaceName,
        "userSubjectId" -> userSubjectId
      )
    )
}
