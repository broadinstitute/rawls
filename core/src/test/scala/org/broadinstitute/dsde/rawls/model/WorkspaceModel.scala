package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.joda.time.DateTime

object Workspace {
  /** convenience constructor that defaults workspace version to v2 and google project to namespace */
  def apply(namespace: String,
            name: String,
            workspaceId: String,
            bucketName: String,
            workflowCollectionName: Option[String],
            createdDate: DateTime,
            lastModified: DateTime,
            createdBy: String,
            attributes: AttributeMap,
            isLocked: Boolean = false): Workspace = {
    new Workspace(namespace, name, workspaceId, bucketName, workflowCollectionName, createdDate, lastModified, createdBy, attributes, isLocked, WorkspaceVersions.V2, GoogleProjectId("blah-todo-change"), Option(GoogleProjectNumber("blah-todo-change")))
  }
}
