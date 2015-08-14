package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.{MethodConfigurationShort, MethodConfiguration}

/**
 * Created by plin on 5/13/15.
 */
trait MethodConfigurationDAO {
  /** gets by method config name*/
  def get(workspaceContext: WorkspaceContext, methodConfigurationNamespace: String, methodConfigurationName: String, txn: RawlsTransaction) : Option[MethodConfiguration]

  /** creates or replaces a method configuration */
  def save(workspaceContext: WorkspaceContext, methodConfiguration: MethodConfiguration, txn: RawlsTransaction) : MethodConfiguration

  /** delete a method configuration, not sure if we need to delete all or a specific version?*/
  def delete(workspaceContext: WorkspaceContext, methodConfigurationNamespace: String, methodConfigurationName: String, txn: RawlsTransaction): Boolean

  /** list all method configurations in the workspace */
  def list(workspaceContext: WorkspaceContext, txn: RawlsTransaction): TraversableOnce[MethodConfigurationShort]

  /** rename method configuration */
  def rename(workspaceContext: WorkspaceContext, methodConfigurationNamespace: String, oldName: String, newName: String, txn: RawlsTransaction): Unit
}
