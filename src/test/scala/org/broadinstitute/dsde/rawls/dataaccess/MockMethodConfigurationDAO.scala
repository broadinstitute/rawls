package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.MethodConfiguration

import scala.collection.mutable

/**
 * Created by plin on 5/13/15.
 */
object MockMethodConfigurationDAO extends MethodConfigurationDAO {
  val store = new mutable.HashMap[(String, String), mutable.HashMap[String, MethodConfiguration]]()

  /** gets by method config name */
  override def get(workspaceNamespace: String, workspaceName: String, methodConfigurationName: String): Option[MethodConfiguration] = {
    store.get(workspaceNamespace, workspaceName).flatMap(_.get(methodConfigurationName))
  }

  /** rename method configuration */
  override def rename(workspaceNamespace: String, workspaceName: String, methodConfigurationName: String, newName: String): Unit = {
    get(workspaceNamespace, workspaceName, methodConfigurationName).foreach { methodConfig =>
      delete(workspaceNamespace, workspaceName, methodConfigurationName)
      save(workspaceNamespace, workspaceName, methodConfig.copy(name = newName))
    }
  }

  /** delete a method configuration, not sure if we need to delete all or a specific version? */
  override def delete(workspaceNamespace: String, workspaceName: String, methodConfigurationName: String): Unit = {
    store.get(workspaceNamespace, workspaceName).flatMap { workspace =>
      workspace.remove(methodConfigurationName)
    }
  }

  /** list all method configurations in the workspace */
  override def list(workspaceNamespace: String, workspaceName: String): TraversableOnce[MethodConfiguration] = {
    store.get(workspaceNamespace, workspaceName).map { workspace =>
      workspace.values
    }
  }.getOrElse(Seq.empty)

  /** creates or replaces a method configuration */
  override def save(workspaceNamespace: String, workspaceName: String, methodConfiguration: MethodConfiguration): MethodConfiguration = {
    store.get(workspaceNamespace, workspaceName).getOrElse({
      store.put((workspaceNamespace, workspaceName), new mutable.HashMap())
      store(workspaceNamespace, workspaceName)
    }).put((methodConfiguration.name), methodConfiguration)
    methodConfiguration
  }

}
