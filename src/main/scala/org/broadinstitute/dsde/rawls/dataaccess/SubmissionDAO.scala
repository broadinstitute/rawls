package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.{WorkspaceName, Submission, Workflow}

/**
 * @author tsharpe
 */
trait SubmissionDAO {
  /** get a submission by workspace and submissionId */
  def get(workspaceContext: WorkspaceContext, submissionId: String, txn: RawlsTransaction): Option[Submission]

  /** list all submissions in the workspace */
  def list(workspaceContext: WorkspaceContext, txn: RawlsTransaction): TraversableOnce[Submission]

  /** create a submission (and its workflows) */
  def save(workspaceContext: WorkspaceContext, submission: Submission, txn: RawlsTransaction): Submission

  /** delete a submission (and its workflows) */
  def delete(workspaceContext: WorkspaceContext, submissionId: String, txn: RawlsTransaction): Boolean

  def update(workspaceContext: WorkspaceContext, submission: Submission, txn: RawlsTransaction): Submission
}

trait WorkflowDAO {
  /** get a workflow by workspace and workflowId */
  def get(workspaceContext: WorkspaceContext, workflowId: String, txn: RawlsTransaction): Option[Workflow]

  /** update a workflow */
  def update(workspaceContext: WorkspaceContext, workflow: Workflow, txn: RawlsTransaction): Workflow

  /** delete a workflow */
  def delete(workspaceContext: WorkspaceContext, workflowId: String, txn: RawlsTransaction): Boolean
}
