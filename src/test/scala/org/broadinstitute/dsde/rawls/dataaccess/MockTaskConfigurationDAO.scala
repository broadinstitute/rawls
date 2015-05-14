package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.TaskConfiguration

import scala.collection.mutable

/**
 * Created by plin on 5/13/15.
 */
object MockTaskConfigurationDAO extends TaskConfigurationDAO {
  val store = new mutable.HashMap[(String, String), mutable.HashMap[String, TaskConfiguration]]()

  /** gets by task config name */
  override def get(workspaceNamespace: String, workspaceName: String, taskConfigurationName: String): Option[TaskConfiguration] = {
    store.get(workspaceNamespace, workspaceName).flatMap(_.get(taskConfigurationName))
  }

  /** rename task configuration */
  override def rename(workspaceNamespace: String, workspaceName: String, taskConfigurationName: String, newName: String): Unit = {
    get(workspaceNamespace, workspaceName, taskConfigurationName).foreach { taskConfig =>
      delete(workspaceNamespace, workspaceName, taskConfigurationName)
      save(workspaceNamespace, workspaceName, taskConfig.copy(name = newName))
    }
  }

  /** delete a task configuration, not sure if we need to delete all or a specific version? */
  override def delete(workspaceNamespace: String, workspaceName: String, taskConfigurationName: String): Unit = {
    store.get(workspaceNamespace, workspaceName).flatMap { workspace =>
      workspace.remove(taskConfigurationName)
    }
  }

  /** list all task configurations in the workspace */
  override def list(workspaceNamespace: String, workspaceName: String): TraversableOnce[TaskConfiguration] = {
    store.get(workspaceNamespace, workspaceName).map { workspace =>
      workspace.values
    }
  }.getOrElse(Seq.empty)

  /** creates or replaces a task configuration */
  override def save(workspaceNamespace: String, workspaceName: String, taskConfiguration: TaskConfiguration): TaskConfiguration = {
    store.get(workspaceNamespace, workspaceName).getOrElse({
      store.put((workspaceNamespace, workspaceName), new mutable.HashMap())
      store(workspaceNamespace, workspaceName)
    }).put((taskConfiguration.name), taskConfiguration)
    taskConfiguration
  }

}
