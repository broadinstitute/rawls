package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.{Submission,Workflow}

/**
 * @author tsharpe
 */
trait SubmissionDAO {
  /** get a submission by workspace and submissionId */
  def get(workspaceNamespace: String, workspaceName: String, submissionId: String, txn: RawlsTransaction): Option[Submission]

  /** list all submissions in the workspace */
  def list(workspaceNamespace: String, workspaceName: String, txn: RawlsTransaction): TraversableOnce[Submission]

  /** create a submission (and its workflows) */
  def save(workspaceNamespace: String, workspaceName: String, submission: Submission, txn: RawlsTransaction): Unit

  /** delete a submission (and its workflows) */
  def delete(workspaceNamespace: String, workspaceName: String, submissionId: String, txn: RawlsTransaction): Boolean

  def update(submission: Submission, txn: RawlsTransaction): Unit
}

trait WorkflowDAO {
  /** get a workflow by workspace and workflowId */
  def get(workspaceNamespace: String, workspaceName: String, workflowId: String, txn: RawlsTransaction): Option[Workflow]

  /** update a workflow */
  def update(workspaceNamespace: String, workspaceName: String, workflow: Workflow, txn: RawlsTransaction): Workflow

  /** delete a workflow */
  def delete(workspaceNamespace: String, workspaceName: String, workflowId: String, txn: RawlsTransaction): Boolean
}
