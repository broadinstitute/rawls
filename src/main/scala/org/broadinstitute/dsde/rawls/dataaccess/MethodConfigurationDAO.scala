package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.MethodConfiguration

/**
 * Created by plin on 5/13/15.
 */
trait MethodConfigurationDAO {
  /** gets by method config name*/
  def get(workspaceNamespace: String, workspaceName: String, methodConfigurationNamespace: String, methodConfigurationName: String, txn: RawlsTransaction) : Option[MethodConfiguration]

  /** creates or replaces a method configuration */
  def save(workspaceNamespace: String, workspaceName: String, methodConfiguration: MethodConfiguration, txn: RawlsTransaction) : MethodConfiguration

  /** delete a method configuration, not sure if we need to delete all or a specific version?*/
  def delete(workspaceNamespace: String, workspaceName: String, methodConfigurationNamespace: String, methodConfigurationName: String, txn: RawlsTransaction)

  /** list all method configurations in the workspace */
  def list(workspaceNamespace: String, workspaceName: String, txn: RawlsTransaction): Seq[MethodConfiguration]

  /** rename method configuration */
  def rename(workspaceNamespace: String, workspaceName: String, methodConfigurationNamespace: String, methodConfiguration: String, newName: String, txn: RawlsTransaction)
}
