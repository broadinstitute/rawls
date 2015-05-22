package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.{MethodConfigurationShort, MethodConfiguration}

import scala.collection.mutable

/**
 * Created by plin on 5/13/15.
 */
object MockMethodConfigurationDAO extends MethodConfigurationDAO {
  val store = new mutable.HashMap[(String, String), mutable.HashMap[(String,String), MethodConfiguration]]()

  /** gets by method config name */
  override def get(workspaceNamespace: String, workspaceName: String, methodConfigurationNamespace: String, methodConfigurationName: String, txn: RawlsTransaction): Option[MethodConfiguration] = {
    store.get(workspaceNamespace, workspaceName).flatMap(_.get(methodConfigurationNamespace, methodConfigurationName))
  }

  /** rename method configuration */
  override def rename(workspaceNamespace: String, workspaceName: String, methodConfigurationNamespace: String, methodConfigurationName: String, newName: String, txn: RawlsTransaction): Unit = {
    get(workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigurationName, txn).foreach { methodConfig =>
      delete(workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigurationName, txn)
      save(workspaceNamespace, workspaceName, methodConfig.copy(name = newName), txn)
    }
  }

  /** delete a method configuration, not sure if we need to delete all or a specific version? */
  override def delete(workspaceNamespace: String, workspaceName: String, methodConfigurationNamespace: String, methodConfigurationName: String, txn: RawlsTransaction): Unit = {
    store.get(workspaceNamespace, workspaceName).flatMap { workspace =>
      workspace.remove(methodConfigurationNamespace, methodConfigurationName)
    }
  }

  /** list all method configurations in the workspace */
  override def list(workspaceNamespace: String, workspaceName: String, txn: RawlsTransaction): TraversableOnce[MethodConfigurationShort] = {
    store.get(workspaceNamespace, workspaceName).map { workspace =>
      workspace.values.map(x => MethodConfigurationShort(x.name, x.rootEntityType, x.method, x.workspaceName, x.namespace))
    }
  }.getOrElse(Seq.empty)

  /** creates or replaces a method configuration */
  override def save(workspaceNamespace: String, workspaceName: String, methodConfiguration: MethodConfiguration, txn: RawlsTransaction): MethodConfiguration = {
    store.get(workspaceNamespace, workspaceName).getOrElse({
      store.put((workspaceNamespace, workspaceName), new mutable.HashMap())
      store(workspaceNamespace, workspaceName)
    }).put((methodConfiguration.namespace, methodConfiguration.name), methodConfiguration)
    methodConfiguration
  }

}
