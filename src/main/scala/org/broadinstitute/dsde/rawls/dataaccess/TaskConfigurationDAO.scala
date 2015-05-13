package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.TaskConfiguration

/**
 * Created by plin on 5/13/15.
 */
trait TaskConfigurationDAO {
  /** gets by task config name*/
  def get(workspaceNamespace: String, workspaceName: String, taskConfigurationName: String) : Option[TaskConfiguration]

  /** gets by task config name*/
  def getByTaskLSID(workspaceNamespace: String, workspaceName: String, taskLSID: String) : Option[TaskConfiguration]

  /** creates or replaces a task configuration */
  def save(workspaceNamespace: String, workspaceName: String, taskConfiguration: TaskConfiguration) : TaskConfiguration

  /** delete a task configuration, not sure if we need to delete all or a specific version?*/
  def delete(workspaceNamespace: String, workspaceName: String, taskConfigurationName: String)

  /** list all task configurations in the workspace */
  def list(workspaceNamespace: String, workspaceName: String): TraversableOnce[TaskConfiguration]

  /** rename task configuration */
  def rename(workspaceNamespace: String, workspaceName: String, taskConfiguration: String, newName: String)
}
