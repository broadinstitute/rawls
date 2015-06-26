package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.{SubmissionStatus,WorkflowStatus}

/**
 * @author tsharpe
 */
trait SubmissionDAO {
  /** get a submission by workspace and submissionId */
  def get(workspaceNamespace: String, workspaceName: String, submissionId: String, txn: RawlsTransaction): Option[SubmissionStatus]

  /** list all submissions in the workspace */
  def list(workspaceNamespace: String, workspaceName: String, txn: RawlsTransaction): TraversableOnce[SubmissionStatus]

  /** create a submission (and its workflows) */
  def save(workspaceNamespace: String, workspaceName: String, submissionStatus: SubmissionStatus, txn: RawlsTransaction): Unit

  /** delete a submission (and its workflows) */
  def delete(workspaceNamespace: String, workspaceName: String, submissionId: String, txn: RawlsTransaction): Boolean
}

trait WorkflowDAO {
  /** get a workflow by workspace and workflowId */
  def get(workspaceNamespace: String, workspaceName: String, workflowId: String, txn: RawlsTransaction): Option[WorkflowStatus]

  /** update a workflow */
  def update(workspaceNamespace: String, workspaceName: String, workflowStatus: WorkflowStatus, txn: RawlsTransaction): Boolean

  /** delete a workflow */
  def delete(workspaceNamespace: String, workspaceName: String, workflowId: String, txn: RawlsTransaction): Boolean
}
